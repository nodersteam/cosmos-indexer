package repository

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rs/zerolog/log"

	"github.com/shopspring/decimal"

	"github.com/nodersteam/cosmos-indexer/db/models"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nodersteam/cosmos-indexer/pkg/model"
)

type Txs interface {
	ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error)
	TransactionsPerPeriod(ctx context.Context, to time.Time) (allTx, all24H, all48H, all30D int64, err error)
	VolumePerPeriod(ctx context.Context, to time.Time) (decimal.Decimal, decimal.Decimal, error)
	Transactions(ctx context.Context, limit int64, offset int64, filter *TxsFilter) ([]*models.Tx, int64, error)
	TransactionRawLog(ctx context.Context, hash string) ([]byte, error)
	TransactionSigners(ctx context.Context, hash string) ([]*models.SignerInfo, error)
	Messages(ctx context.Context, hash string) ([]*models.Message, error)
	GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error)
	GetWalletsCount(ctx context.Context) (*model.TotalWallets, error)
	ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error)
	ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error)
}

type TxsFilter struct {
	TxHash        *string
	TxBlockHeight *int64
}

type txs struct {
	db *pgxpool.Pool
}

func NewTxs(db *pgxpool.Pool) *txs {
	return &txs{db: db}
}

func (r *txs) ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error) {
	query := `
				select count(txes.hash),  date_trunc('hour', txes.timestamp) from txes
				where txes.timestamp >= $1 and txes.timestamp <= $2
				group by date_trunc('hour', txes.timestamp)
				`
	rows, err := r.db.Query(ctx, query, to.UTC().Add(-24*time.Hour), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := make([]*model.TxsByHour, 0)
	for rows.Next() {
		var in model.TxsByHour
		errScan := rows.Scan(&in.TxNum, &in.Hour)
		if errScan != nil {
			return nil, fmt.Errorf("repository.ChartTransactionsByHour, Scan: %v", errScan)
		}
		points = append(points, &in)
	}

	all24H, err := r.txCountPerPeriod(ctx, to.UTC().Add(-24*time.Hour), to.UTC())
	if err != nil {
		return nil, err
	}

	all48H, err := r.txCountPerPeriod(ctx, to.UTC().Add(-48*time.Hour), to.UTC().Add(-24*time.Hour))
	if err != nil {
		return nil, err
	}

	return &model.TxByHourWithCount{
		Points:   points,
		Total24H: all24H,
		Total48H: all48H,
	}, nil
}

func (r *txs) ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error) {
	query := `select SUM(fs.amount), date_trunc('hour', txes.timestamp)
    		from txes
    		left join fees fs on fs.tx_id = txes.id
			left join denoms dm on fs.denomination_id = dm.id
			where txes.timestamp between $1 AND $2
			group by date_trunc('hour', txes.timestamp)`
	rows, err := r.db.Query(ctx, query, to.UTC().Add(-24*time.Hour), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := make([]*model.TxVolumeByHour, 0)
	for rows.Next() {
		var in model.TxVolumeByHour
		errScan := rows.Scan(&in.TxVolume, &in.Hour)
		if errScan != nil {
			return nil, fmt.Errorf("repository.ChartTransactionsVolume, Scan: %v", errScan)
		}
		points = append(points, &in)
	}
	return points, nil
}

func (r *txs) ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error) {
	query := `
				select count(txes.hash),  date_trunc('day', txes.timestamp) from txes
				where txes.timestamp >= $1 and txes.timestamp <= $2
				group by date_trunc('day', txes.timestamp)
				`
	data := make([]*model.TxsByDay, 0)
	rows, err := r.db.Query(ctx, query, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var in model.TxsByDay
		errScan := rows.Scan(&in.TxNum, &in.Day)
		if errScan != nil {
			return nil, fmt.Errorf("repository.ChartTxByDay, Scan: %v", errScan)
		}
		data = append(data, &in)
	}

	return data, nil
}

func (r *txs) TransactionsPerPeriod(ctx context.Context, to time.Time) (allTx,
	all24H, all48H, all30D int64, err error) {

	query := `select count(*) from txes`
	row := r.db.QueryRow(ctx, query)
	if err := row.Scan(&allTx); err != nil {
		return 0, 0, 0, 0, err
	}

	from := to.UTC().Truncate(24 * time.Hour)
	all24H, err = r.txCountPerPeriod(ctx, from, to)
	if err != nil {
		log.Err(err).Msg("repository.TransactionsPerPeriod txCountPerPeriod")
		return 0, 0, 0, 0, err
	}

	all48H, err = r.txCountPerPeriod(ctx, from.Add(-24*time.Hour), from)
	if err != nil {
		log.Err(err).Msg("repository.TransactionsPerPeriod txCountPerPeriod")
		return 0, 0, 0, 0, err
	}

	from = to.UTC().Add(-720 * time.Hour).Truncate(24 * time.Hour)
	all30D, err = r.txCountPerPeriod(ctx, from, to)
	if err != nil {
		log.Err(err).Msg("repository.TransactionsPerPeriod txCountPerPeriod")
		return 0, 0, 0, 0, err
	}

	return allTx, all24H, all48H, all30D, nil
}

func (r *txs) txCountPerPeriod(ctx context.Context, from, to time.Time) (int64, error) {
	query := `select COALESCE(count(*),0) from txes where txes.timestamp between $1 AND $2`
	row := r.db.QueryRow(ctx, query, from.UTC(), to.UTC())
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

func (r *txs) VolumePerPeriod(ctx context.Context, to time.Time) (decimal.Decimal, decimal.Decimal, error) {
	total24H, err := r.volumePerPeriod(ctx, to.Add(-24*time.Hour), to)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}

	total30D, err := r.volumePerPeriod(ctx, to.Add(-(24*30)*time.Hour).UTC(), to.UTC())
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}

	return total24H, total30D, nil
}

func (r *txs) volumePerPeriod(ctx context.Context, from, to time.Time) (decimal.Decimal, error) {
	query := `select COALESCE(SUM(fs.amount),0)
    		from txes
    		left join fees fs on fs.tx_id = txes.id
			left join denoms dm on fs.denomination_id = dm.id
			where txes.timestamp between $1 AND $2`
	row := r.db.QueryRow(ctx, query, from.UTC(), to.UTC())
	var total decimal.Decimal
	if err := row.Scan(&total); err != nil {
		return decimal.Zero, err
	}
	return total, nil
}

func (r *txs) TransactionSigners(ctx context.Context, txHash string) ([]*models.SignerInfo, error) {
	querySignerInfos := `
						select
							txi.signer_info_id,
							txnf.address_id,
							coalesce(txnf.mode_info,''),
							coalesce(txnf.sequence, 0),
							addr.address
						from txes
							inner join tx_auth_info tai on txes.auth_info_id = txes.id
							inner join tx_signer_infos txi on tai.id = txi.auth_info_id
							inner join tx_signer_info txnf on txi.signer_info_id = txnf.id
							inner join addresses addr on txnf.address_id = addr.id
						where txes.hash = $1`
	signerInfos := make([]*models.SignerInfo, 0)
	rowsSigners, err := r.db.Query(ctx, querySignerInfos, txHash)
	if err != nil {
		log.Err(err).Msgf("querySignerInfos error")
		return nil, err
	}
	for rowsSigners.Next() {
		var in models.SignerInfo
		var addr models.Address

		errScan := rowsSigners.Scan(&in.ID, &addr.ID, &in.ModeInfo, &in.Sequence, &addr.Address)
		if errScan != nil {
			log.Err(err).Msgf("rowsSigners.Scan error")
			return nil, fmt.Errorf("repository.querySignerInfos, Scan: %v", errScan)
		}
		in.Address = &addr
		signerInfos = append(signerInfos, &in)
	}

	return signerInfos, nil
}

func (r *txs) TransactionRawLog(ctx context.Context, hash string) ([]byte, error) {
	query := `
	   select
		   resp.raw_log
		from txes
			 inner join tx_responses resp on txes.tx_response_id = resp.id
		where txes.hash = $1`

	row := r.db.QueryRow(ctx, query, hash)

	var rawLog []byte
	if err := row.Scan(&rawLog); err != nil {
		log.Err(err).Msgf("repository.TransactionRawLog")
		return nil, fmt.Errorf("not found")
	}

	return rawLog, nil
}

func (r *txs) Transactions(ctx context.Context, limit int64, offset int64, filter *TxsFilter) ([]*models.Tx, int64, error) {
	query := `
	   select 
	       txes.id as tx_id,
	       txes.signatures as signatures,
		   txes.hash,
		   txes.code as tx_code,
		   txes.block_id,
		   txes.timestamp,
		   txes.memo,
		   txes.timeout_height,
		   txes.extension_options,
		   txes.non_critical_extension_options,
		   txes.auth_info_id,
		   txes.tx_response_id,
		   COALESCE(auf.gas_limit,0), 
		   COALESCE(auf.payer,''), 
		   COALESCE(auf.granter,''), 
		   COALESCE(tip.tipper,''),
		   COALESCE(resp.code,0) as tx_resp_code,
		   COALESCE(resp.gas_used,0) as tx_res_gas_used,
		   COALESCE(resp.gas_wanted,0) as tx_res_gas_wanted,
		   COALESCE(resp.time_stamp, '2000-01-01 00:00:00'), 
		   COALESCE(resp.codespace,''), 
		   COALESCE(resp.data,''), 
		   COALESCE(resp.info,'')
		from txes
			 left join tx_auth_info au on auth_info_id = au.id
			 left join tx_auth_info_fee auf on au.fee_id = auf.id
			 left join tx_tip tip on au.tip_id = tip.id
			 left join tx_responses resp on tx_response_id = resp.id
			 left join blocks on txes.block_id = blocks.id`

	var rows pgx.Rows
	var err error
	if filter != nil { // TODO make it more flexible
		if filter.TxBlockHeight != nil {
			query += ` WHERE blocks.height = $1`
			query += ` ORDER BY txes.timestamp desc LIMIT $2 OFFSET $3`
			rows, err = r.db.Query(ctx, query, *filter.TxBlockHeight, limit, offset)
		} else if filter.TxHash != nil && len(*filter.TxHash) > 0 {
			query += ` WHERE hash = $1`
			query += ` ORDER BY txes.timestamp desc LIMIT $2 OFFSET $3`
			rows, err = r.db.Query(ctx, query, *filter.TxHash, limit, offset)
		}
	} else {
		query += ` ORDER BY txes.timestamp desc LIMIT $1 OFFSET $2`
		rows, err = r.db.Query(ctx, query, limit, offset)
	}

	if err != nil {
		log.Err(err).Msgf("Query error")
		return nil, 0, err
	}

	result := make([]*models.Tx, 0)
	for rows.Next() {
		var tx models.Tx
		var authInfo models.AuthInfo
		var authInfoFee models.AuthInfoFee
		var authInfoTip models.Tip
		var txResponse models.TxResponse
		signatures := make([][]byte, 0)
		extensionsOptions := make([]string, 0)
		nonCriticalExtensionOptions := make([]string, 0)

		if err := rows.Scan(&tx.ID, &signatures, &tx.Hash, &tx.Code,
			&tx.BlockID, &tx.Timestamp, &tx.Memo, &tx.TimeoutHeight,
			&extensionsOptions, &nonCriticalExtensionOptions,
			&tx.AuthInfoID, &tx.TxResponseID,
			&authInfoFee.GasLimit, &authInfoFee.Payer,
			&authInfoFee.Granter, &authInfoTip.Tipper,
			&txResponse.Code, &txResponse.GasUsed,
			&txResponse.GasWanted,
			&txResponse.TimeStamp, &txResponse.Codespace,
			&txResponse.Data, &txResponse.Info); err != nil {
			log.Err(err).Msgf("rows.Scan error")
			return nil, 0, err
		}
		tx.Signatures = signatures
		tx.ExtensionOptions = extensionsOptions
		tx.NonCriticalExtensionOptions = nonCriticalExtensionOptions

		var block *models.Block
		if block, err = r.blockInfo(ctx, tx.BlockID); err != nil {
			log.Err(err).Msgf("error in blockInfo")
		}
		if block != nil {
			tx.Block = *block
		}

		var fees []models.Fee
		if fees, err = r.feesByTransaction(ctx, tx.ID); err != nil {
			log.Err(err).Msgf("error in feesByTransaction")
		}
		tx.Fees = fees

		authInfo.Fee = authInfoFee
		authInfo.Tip = authInfoTip
		tx.AuthInfo = authInfo
		tx.TxResponse = txResponse

		res, err := r.GetSenderAndReceiver(context.Background(), tx.Hash)
		if err == nil {
			tx.SenderReceiver = res
		}

		result = append(result, &tx)
	}

	blockID := -1
	if filter != nil && filter.TxBlockHeight != nil {
		queryBlock := `select id from blocks where blocks.height = $1`
		row := r.db.QueryRow(ctx, queryBlock, *filter.TxBlockHeight)
		if err = row.Scan(&blockID); err != nil {
			log.Err(err).Msgf("queryBlock error")
		}
	}

	var row pgx.Row
	if blockID >= 0 {
		queryAll := `select count(*) from txes where txes.block_id = $1`
		row = r.db.QueryRow(ctx, queryAll, blockID)
	} else {
		queryAll := `select count(*) from txes`
		row = r.db.QueryRow(ctx, queryAll)
	}

	var allTx int64
	if err = row.Scan(&allTx); err != nil {
		log.Err(err).Msgf("queryAll error")
		return nil, 0, err
	}

	return result, allTx, nil
}

func (r *txs) feesByTransaction(ctx context.Context, txID uint) ([]models.Fee, error) {
	feesQ := `select fs.amount, dm.base, a.address from fees fs 
    				left join public.addresses a on fs.payer_address_id = a.id
    				left join denoms dm on fs.denomination_id = dm.id
    				where fs.tx_id = $1`
	rowsFees, err := r.db.Query(ctx, feesQ, txID)
	if err != nil {
		log.Err(err).Msgf("feesRes error")
		return nil, err
	}
	feesRes := make([]models.Fee, 0)
	for rowsFees.Next() {
		var in models.Fee
		var denom models.Denom
		var address models.Address

		errScan := rowsFees.Scan(&in.Amount, &denom.Base, &address.Address)
		if errScan != nil {
			log.Err(err).Msgf("rowsFees.Scan error")
			return nil, fmt.Errorf("repository.feesByTransaction, Scan: %v", errScan)
		}
		in.Denomination = denom
		in.PayerAddress = address
		feesRes = append(feesRes, in)
	}
	return feesRes, nil
}

func (r *txs) blockInfo(ctx context.Context, blockID uint) (*models.Block, error) {
	queryBlockInfo := `
						select 
						    blocks.time_stamp, 
						    blocks.height, 
						    blocks.chain_id, 
						    addresses.address, 
						    blocks.block_hash
							from blocks 
							left join addresses on blocks.proposer_cons_address_id = addresses.id
							where blocks.id = $1
						`
	var block models.Block
	var address models.Address
	rowBlock := r.db.QueryRow(ctx, queryBlockInfo, blockID)
	if err := rowBlock.Scan(&block.TimeStamp, &block.Height, &block.ChainID, &address.Address, &block.BlockHash); err != nil {
		log.Err(err).Msgf("rowBlock.Scan error")
		return nil, err
	}
	block.ProposerConsAddress = address
	return &block, nil
}

func (r *txs) Messages(ctx context.Context, hash string) ([]*models.Message, error) {
	query := `select txes.id,
       messages.message_index,
       messages.message_bytes,
       message_types.message_type
		from txes
		   left join messages on txes.id = messages.tx_id
		   left join message_types on messages.message_type_id = message_types.id
		   where txes.hash = $1`
	rows, err := r.db.Query(ctx, query, hash)
	if err != nil {
		log.Err(err).Msgf("rows error")
		return nil, err
	}
	res := make([]*models.Message, 0)
	for rows.Next() {
		var txID uint
		var messageIndex int
		var messageBytes []byte
		var messageType string

		if err := rows.Scan(&txID, &messageIndex, &messageBytes, &messageType); err != nil {
			log.Err(err).Msgf("Messages: rows.Scan error")
			return nil, err
		}

		res = append(res, &models.Message{
			TxID:         txID,
			MessageIndex: messageIndex,
			MessageBytes: messageBytes,
			MessageType:  models.MessageType{MessageType: messageType},
		})
	}
	return res, nil
}

func (r *txs) GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error) {
	query := `select
		   message_types.message_type,
		   message_event_attributes.value,
		   message_event_attribute_keys.key
	from txes
			left join messages on txes.id = messages.tx_id
			left join message_types on messages.message_type_id = message_types.id
			left join message_events on messages.id = message_events.message_id
			left join message_event_types on message_events.message_event_type_id=message_event_types.id
			left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
			left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
		   where txes.hash = $1
		   and message_event_types.type = ANY($2)
	order by messages.message_index, message_events.index, message_event_attributes.index asc`
	types := []string{"transfer", "fungible_token_packet"}
	rows, err := r.db.Query(ctx, query, hash, types)
	if err != nil {
		log.Err(err).Msgf("GetSenderAndReceiver: rows error")
		return nil, err
	}

	res := &model.TxSenderReceiver{}
	for rows.Next() {
		var messageType string
		var key string
		var value string
		if err := rows.Scan(&messageType, &value, &key); err != nil {
			return nil, err
		}
		if res.MessageType == "" {
			res.MessageType = messageType
		}

		if strings.EqualFold(key, "sender") {
			res.Sender = value
		}

		if strings.EqualFold(key, "recipient") || strings.EqualFold(key, "receiver") {
			res.Receiver = value
		}

		if strings.EqualFold(key, "amount") {
			amount, denom, err := r.extractNumber(value)
			if err != nil {
				log.Err(err).Msgf("GetSenderAndReceiver: extractNumber error")
				res.Amount = value
			} else {
				res.Amount = amount.String()
				res.Denom = denom
			}
		}
	}
	return res, nil
}

func (r *txs) extractNumber(value string) (decimal.Decimal, string, error) {
	pattern := regexp.MustCompile(`(\d+)`)
	numberStrings := pattern.FindAllStringSubmatch(value, -1)
	numbers := make([]int, len(numberStrings))
	for i, numberString := range numberStrings {
		number, err := strconv.Atoi(numberString[1])
		if err != nil {
			return decimal.Zero, "", err
		}
		numbers[i] = number
	}
	if len(numbers) > 0 {
		denom := strings.ReplaceAll(value, strconv.Itoa(numbers[0]), "")
		return decimal.NewFromInt(int64(numbers[0])), denom, nil
	}

	return decimal.Zero, "", fmt.Errorf("not found")
}

func (r *txs) GetWalletsCount(ctx context.Context) (*model.TotalWallets, error) {
	query := `select distinct
			   count(message_event_attributes.value) as total
			from txes
					 left join messages on txes.id = messages.tx_id
					 left join message_types on messages.message_type_id = message_types.id
					 left join message_events on messages.id = message_events.message_id
					 left join message_event_types on message_events.message_event_type_id=message_event_types.id
					 left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
					 left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
			where message_event_attribute_keys.key = ANY($2)
			and date(txes.timestamp) = date($1)`
	types := []string{"sender"}
	row := r.db.QueryRow(ctx, query, time.Now().UTC(), types)
	var count24H int64
	if err := row.Scan(&count24H); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		count24H = 0
	}

	row = r.db.QueryRow(ctx, query, time.Now().UTC().Add(-24*time.Hour), types)
	var count48H int64
	if err := row.Scan(&count48H); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		count48H = 0
	}

	queryAll := `select distinct
		   count(message_event_attributes.value) as total
		from txes
				 left join messages on txes.id = messages.tx_id
				 left join message_types on messages.message_type_id = message_types.id
				 left join message_events on messages.id = message_events.message_id
				 left join message_event_types on message_events.message_event_type_id=message_event_types.id
				 left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
				 left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
		where message_event_attribute_keys.key = ANY($1)`
	types = append(types, "receiver", "recipient")
	row = r.db.QueryRow(ctx, queryAll, types)
	var countAll int64
	if err := row.Scan(&countAll); err != nil {
		return nil, err
	}

	return &model.TotalWallets{Total: countAll, Count24H: count24H, Count48H: count48H}, nil
}
