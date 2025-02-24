package repository

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noders-team/cosmos-indexer/db/models"
	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

type Txs interface {
	ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error)
	TransactionsPerPeriod(ctx context.Context, to time.Time) (allTx, all24H, all48H, all30D int64, err error)
	VolumePerPeriod(ctx context.Context, to time.Time) (decimal.Decimal, decimal.Decimal, error)
	Transactions(ctx context.Context, limit int64, offset int64, filter *TxsFilter) ([]*model.Tx, int64, error)
	TransactionRawLog(ctx context.Context, hash string) ([]byte, error)
	TransactionSigners(ctx context.Context, hash string) ([]*model.SignerInfo, error)
	Messages(ctx context.Context, hash string) ([]*model.Message, error)
	GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error)
	GetSenderAndReceiverV2(ctx context.Context, hash string) (*model.TxSenderReceiver, error)
	GetWalletsCount(ctx context.Context) (*model.TotalWallets, error)
	ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error)
	ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error)
	GetPowerEvents(ctx context.Context, accountAddress string,
		limit int64, offset int64) ([]*model.Tx, int64, error)
	GetValidatorHistory(ctx context.Context, accountAddress string,
		limit int64, offset int64) ([]*model.Tx, int64, error)
	TransactionsByEventValue(ctx context.Context, values []string, messageType []string, includeEvents bool,
		limit int64, offset int64) ([]*model.Tx, int64, error)
	GetVotes(ctx context.Context, accountAddress string, uniqueProposals bool, limit int64, offset int64) ([]*model.VotesTransaction, int64, error)
	GetVotesByAccounts(ctx context.Context, accounts []string, excludeAccounts bool, voteOption string,
		proposalID int, byAccAddress *string, limit int64, offset int64, sortBy *model.SortBy) ([]*model.VotesTransaction, int64, error)
	GetWalletsCountPerPeriod(ctx context.Context, startDate, endDate time.Time) (int64, error)
	GetWalletsWithTx(ctx context.Context, limit int64, offset int64) ([]*model.WalletWithTxs, int64, error)
	TxCountByAccounts(ctx context.Context, accounts []string) ([]*model.WalletWithTxs, error)
	AccountInfo(ctx context.Context, account string) (*model.AccountInfo, error)
	GetEvents(ctx context.Context, txID uint) ([]*model.TxEvents, error)
	GetEventsV2(ctx context.Context, txHash string) ([]*model.TxEvents, error)
	UpdateViews(ctx context.Context) error
	ExtractNumber(value string) (decimal.Decimal, string, error)
	DelegatesByValidator(ctx context.Context, from, to time.Time, valoperAddress string,
		limit int64, offset int64) (data []*model.Tx, totalSum *model.Denom, all int64, err error)
	ProposalDepositors(ctx context.Context, proposalID int,
		sortBy *model.SortBy, limit int64, offset int64) ([]*model.ProposalDeposit, int64, error)
	TotalRewardByAccount(ctx context.Context, account string) ([]*model.DecCoin, error)
}

type TxsFilter struct {
	TxHash        *string
	TxBlockHeight *int64
	TxHashes      []string
}

type txs struct {
	db *pgxpool.Pool
}

func NewTxs(db *pgxpool.Pool) Txs {
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
	all24H, all48H, all30D int64, err error,
) {
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

func (r *txs) TransactionSigners(ctx context.Context, txHash string) ([]*model.SignerInfo, error) {
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
	signerInfos := make([]*model.SignerInfo, 0)
	rowsSigners, err := r.db.Query(ctx, querySignerInfos, txHash)
	if err != nil {
		log.Err(err).Msgf("querySignerInfos error")
		return nil, err
	}
	for rowsSigners.Next() {
		var in model.SignerInfo
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

func (r *txs) Transactions(ctx context.Context, limit int64, offset int64, filter *TxsFilter) ([]*model.Tx, int64, error) {
	dialect := goqu.Select(
		goqu.I("txes.id").As("tx_id"),
		goqu.I("txes.signatures").As("signatures"),
		goqu.I("txes.hash"),
		goqu.I("txes.code").As("tx_code"),
		goqu.I("txes.block_id"),
		goqu.I("txes.timestamp"),
		goqu.I("txes.memo"),
		goqu.I("txes.timeout_height"),
		goqu.I("txes.extension_options"),
		goqu.I("txes.non_critical_extension_options"),
		goqu.I("txes.auth_info_id"),
		goqu.I("txes.tx_response_id"),
		goqu.COALESCE(goqu.I("auf.gas_limit"), 0),
		goqu.COALESCE(goqu.I("auf.payer"), ""),
		goqu.COALESCE(goqu.I("auf.granter"), ""),
		goqu.COALESCE(goqu.I("tip.tipper"), ""),
		goqu.COALESCE(goqu.I("resp.code"), 0).As("tx_resp_code"),
		goqu.COALESCE(goqu.I("resp.gas_used"), 0).As("tx_res_gas_used"),
		goqu.COALESCE(goqu.I("resp.gas_wanted"), 0).As("tx_res_gas_wanted"),
		goqu.COALESCE(goqu.I("resp.time_stamp"), time.Now()),
		goqu.COALESCE(goqu.I("resp.codespace"), ""),
		goqu.COALESCE(goqu.I("resp.data"), ""),
		goqu.COALESCE(goqu.I("resp.info"), "")).
		From("txes").
		LeftJoin(
			goqu.T("tx_auth_info").As("au"),
			goqu.On(goqu.Ex{"auth_info_id": goqu.I("au.id")}),
		).
		LeftJoin(
			goqu.T("tx_auth_info_fee").As("auf"),
			goqu.On(goqu.Ex{"au.fee_id": goqu.I("auf.id")}),
		).
		LeftJoin(
			goqu.T("tx_tip").As("tip"),
			goqu.On(goqu.Ex{"au.tip_id": goqu.I("tip.id")}),
		).
		LeftJoin(
			goqu.T("tx_responses").As("resp"),
			goqu.On(goqu.Ex{"tx_response_id": goqu.I("resp.id")}),
		).
		LeftJoin(
			goqu.T("blocks"),
			goqu.On(goqu.Ex{"txes.block_id": goqu.I("blocks.id")}),
		)

	log.Info().Msgf("=> Transactions ==> filter.TxHashes: %v", filter)
	if filter != nil {
		if filter.TxBlockHeight != nil {
			dialect = dialect.Where(goqu.I("blocks.height").Eq(*filter.TxBlockHeight))
		} else if filter.TxHash != nil && len(*filter.TxHash) > 0 {
			dialect = dialect.Where(goqu.I("txes.hash").Eq(*filter.TxHash))
		} else if len(filter.TxHashes) > 0 {
			log.Info().Msgf("=> Transactions ==> filter.TxHashes: %v", filter.TxHashes)
			dialect = dialect.Where(goqu.I("txes.hash").In(filter.TxHashes))
		}
	}

	dialect = dialect.
		Order(goqu.I("txes.timestamp").Desc()).
		Limit(uint(limit)).Offset(uint(offset))

	query, args, err := dialect.ToSQL()
	if err != nil {
		log.Err(err).Msgf("Transactions Query builder error")
		return nil, 0, err
	}

	log.Debug().Msgf("=> Transactions ==> query: %s", query)

	startTime := time.Now()
	log.Debug().Msgf("=> TransactionsByEventValue ==> transaction main start: %s", startTime.String())
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Err(err).Msgf("Transactions Query error")
		return nil, 0, err
	}
	log.Debug().Msgf("=> TransactionsByEventValue ==> transaction main finish: %s", time.Since(startTime).String())

	result := make([]*model.Tx, 0)
	if rows != nil {
		for rows.Next() {
			var tx model.Tx
			var authInfo model.AuthInfo
			var authInfoFee model.AuthInfoFee
			var authInfoTip model.Tip
			var txResponse model.TxResponse
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

			startTime := time.Now()
			log.Debug().Msgf("=> TransactionsByEventValue ==> transaction loop start: %s", startTime.String())

			var block *models.Block
			if block, err = r.blockInfo(ctx, tx.BlockID); err != nil {
				log.Err(err).Msgf("error in blockInfo")
			}
			if block != nil {
				tx.Block = *block
			}
			log.Debug().Msgf("=> TransactionsByEventValue ==> transaction loop block: %s", time.Since(startTime).String())

			startTime = time.Now()
			var fees []model.Fee
			if fees, err = r.feesByTransaction(ctx, tx.ID); err != nil {
				log.Err(err).Msgf("error in feesByTransaction")
			}
			tx.Fees = fees
			log.Debug().Msgf("=> TransactionsByEventValue ==> transaction loop fees: %s", time.Since(startTime).String())

			authInfo.Fee = authInfoFee
			authInfo.Tip = authInfoTip

			tx.AuthInfo = authInfo
			tx.TxResponse = txResponse

			startTime = time.Now()
			res, err := r.GetSenderAndReceiverV2(context.Background(), tx.Hash)
			if err == nil {
				tx.SenderReceiver = res
			}
			log.Debug().Msgf("=> TransactionsByEventValue ==> transaction loop GetSenderAndReceiverV2: %s", time.Since(startTime).String())

			result = append(result, &tx)
		}
	}

	blockID := -1
	if filter != nil && filter.TxBlockHeight != nil {
		queryBlock := `select id from blocks where blocks.height = $1`
		row := r.db.QueryRow(ctx, queryBlock, *filter.TxBlockHeight)
		if err = row.Scan(&blockID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			log.Err(err).Msgf("queryBlock error")
			return nil, 0, err
		}
	}

	startTime = time.Now()
	log.Debug().Msgf("=> TransactionsByEventValue ==> transaction total: %s", startTime.String())

	var row pgx.Row
	if blockID >= 0 {
		queryAll := `select count(*) from txes where txes.block_id = $1`
		row = r.db.QueryRow(ctx, queryAll, blockID)
	} else {
		queryAll := `select count(*) from txes`
		row = r.db.QueryRow(ctx, queryAll)
	}

	var allTx int64
	if err = row.Scan(&allTx); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Err(err).Msgf("queryAll error")
		return nil, 0, err
	}
	log.Debug().Msgf("=> TransactionsByEventValue ==> transaction total finish: %s", time.Since(startTime).String())

	return result, allTx, nil
}

func (r *txs) feesByTransaction(ctx context.Context, txID uint) ([]model.Fee, error) {
	feesQ := `select fs.amount, dm.base, a.address from fees fs 
    				left join public.addresses a on fs.payer_address_id = a.id
    				left join denoms dm on fs.denomination_id = dm.id
    				where fs.tx_id = $1`
	rowsFees, err := r.db.Query(ctx, feesQ, txID)
	if err != nil {
		log.Err(err).Msgf("feesRes error")
		return nil, err
	}
	feesRes := make([]model.Fee, 0)
	for rowsFees.Next() {
		var in model.Fee
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

func (r *txs) Messages(ctx context.Context, hash string) ([]*model.Message, error) {
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
	res := make([]*model.Message, 0)
	for rows.Next() {
		var txID uint
		var messageIndex int
		var messageBytes []byte
		var messageType string

		if err := rows.Scan(&txID, &messageIndex, &messageBytes, &messageType); err != nil {
			log.Err(err).Msgf("Messages: rows.Scan error")
			return nil, err
		}

		res = append(res, &model.Message{
			TxID:         txID,
			MessageIndex: messageIndex,
			MessageBytes: messageBytes,
			MessageType:  model.MessageType{MessageType: messageType},
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
	types := []string{"transfer", "fungible_token_packet", "delegate", "coin_received", "coin_spent"}
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

		if strings.EqualFold(key, "sender") || strings.EqualFold(key, "spender") {
			res.Sender = value
		}

		if strings.EqualFold(key, "recipient") || strings.EqualFold(key, "receiver") {
			res.Receiver = value
		}

		if strings.EqualFold(key, "amount") {
			amount, denom, err := r.ExtractNumber(value)
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

func (r *txs) GetSenderAndReceiverV2(ctx context.Context, hash string) (*model.TxSenderReceiver, error) {
	query := `select
		   txes.message_type,
		   txes.message_event_attr_value,
		   txes.message_event_attr_key
	from tx_events_aggregateds as txes where txes.tx_hash = $1
		   and txes.message_event_type = ANY($2)
	order by txes.message_type_index, txes.message_event_attr_index`
	types := []string{"transfer", "fungible_token_packet", "delegate", "coin_received", "coin_spent"}
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

		if strings.EqualFold(key, "sender") || strings.EqualFold(key, "spender") {
			res.Sender = value
		}

		if strings.EqualFold(key, "recipient") || strings.EqualFold(key, "receiver") {
			res.Receiver = value
		}

		if strings.EqualFold(key, "amount") {
			amount, denom, err := r.ExtractNumber(value)
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

func (r *txs) ExtractNumber(value string) (decimal.Decimal, string, error) {
	pattern := regexp.MustCompile(`(\d+)`)
	numberStrings := pattern.FindAllStringSubmatch(value, -1)
	numbers := make([]int64, len(numberStrings))
	for i, numberString := range numberStrings {
		number, err := decimal.NewFromString(numberString[1])
		if err != nil {
			return decimal.Zero, "", err
		}
		numbers[i] = number.CoefficientInt64()
	}
	if len(numbers) > 0 {
		denom := strings.ReplaceAll(value, strconv.Itoa(int(numbers[0])), "")
		return decimal.NewFromInt(numbers[0]), denom, nil
	}

	return decimal.Zero, "", fmt.Errorf("not found")
}

func (r *txs) GetWalletsCount(ctx context.Context) (*model.TotalWallets, error) {
	query := `select count(distinct account) from transactions_normalized`
	queryPerDate := query + ` where date(time) = date($1)`
	row := r.db.QueryRow(ctx, queryPerDate, time.Now().UTC())
	var count24H int64
	if err := row.Scan(&count24H); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		count24H = 0
	}

	row = r.db.QueryRow(ctx, queryPerDate, time.Now().UTC().Add(-24*time.Hour))
	var count48H int64
	if err := row.Scan(&count48H); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		count48H = 0
	}

	queryMoreDate := query + ` where date(time) >= date($1)`
	firstDay := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.Local)
	row = r.db.QueryRow(ctx, queryMoreDate, firstDay)
	var count30D int64
	if err := row.Scan(&count30D); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		count30D = 0
	}

	// total wallets
	queryAll := `select count(distinct account) from transactions_normalized`
	row = r.db.QueryRow(ctx, queryAll)
	var countAll int64
	if err := row.Scan(&countAll); err != nil {
		return nil, err
	}

	return &model.TotalWallets{Total: countAll, Count24H: count24H, Count48H: count48H, Count30D: count30D}, nil
}

func (r *txs) GetWalletsCountPerPeriod(ctx context.Context, startDate, endDate time.Time) (int64, error) {
	query := `select count(distinct account) from transactions_normalized where date(time) BETWEEN date($1) and date($2)`
	row := r.db.QueryRow(ctx, query, startDate.UTC(), endDate.UTC())
	var count int64
	if err := row.Scan(&count); err != nil {
		log.Err(err).Msgf("GetWalletsCount: rows error")
		return 0, err
	}

	return count, nil
}

func (r *txs) GetWalletsWithTx(ctx context.Context, limit int64, offset int64) ([]*model.WalletWithTxs, int64, error) {
	query := `SELECT
		subquery.value,
		subquery.tx_count
	FROM (
			 SELECT
				 message_event_attributes.value,
				 COUNT(DISTINCT txes.hash) AS tx_count,
				 txes.timestamp
			 FROM txes
					  LEFT JOIN messages ON txes.id = messages.tx_id
					  LEFT JOIN message_types ON messages.message_type_id = message_types.id
					  LEFT JOIN message_events ON messages.id = message_events.message_id
					  LEFT JOIN message_event_types ON message_events.message_event_type_id = message_event_types.id
					  LEFT JOIN message_event_attributes ON message_events.id = message_event_attributes.message_event_id
					  LEFT JOIN message_event_attribute_keys ON message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
			 WHERE message_event_attribute_keys.key = ANY($1)
			 GROUP BY message_event_attributes.value, txes.timestamp
		 ) subquery
	ORDER BY subquery.timestamp DESC LIMIT $2 OFFSET $3`
	types := []string{"sender"}

	rows, err := r.db.Query(ctx, query, types, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	data := make([]*model.WalletWithTxs, 0)
	for rows.Next() {
		var item model.WalletWithTxs
		if err = rows.Scan(&item.Account, &item.TxCount); err != nil {
			return nil, 0, err
		}
		data = append(data, &item)
	}

	queryAll := `SELECT
				 COUNT(message_event_attributes.value)
			 FROM txes
					  LEFT JOIN messages ON txes.id = messages.tx_id
					  LEFT JOIN message_types ON messages.message_type_id = message_types.id
					  LEFT JOIN message_events ON messages.id = message_events.message_id
					  LEFT JOIN message_event_types ON message_events.message_event_type_id = message_event_types.id
					  LEFT JOIN message_event_attributes ON message_events.id = message_event_attributes.message_event_id
					  LEFT JOIN message_event_attribute_keys ON message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
			 WHERE message_event_attribute_keys.key = ANY($1)`
	var total int64
	if err = r.db.QueryRow(ctx, queryAll, types).Scan(&total); err != nil {
		return nil, 0, err
	}

	return data, total, nil
}

func (r *txs) TxCountByAccounts(ctx context.Context, accounts []string) ([]*model.WalletWithTxs, error) {
	query := `SELECT account, count(distinct tx_hash) from transactions_normalized where account=ANY($1) GROUP BY account`
	rows, err := r.db.Query(ctx, query, accounts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := make([]*model.WalletWithTxs, 0)
	for rows.Next() {
		var item model.WalletWithTxs
		if err = rows.Scan(&item.Account, &item.TxCount); err != nil {
			return nil, err
		}
		data = append(data, &item)
	}

	return data, nil
}

func (r *txs) GetValidatorHistory(ctx context.Context, accountAddress string, limit int64, offset int64) ([]*model.Tx, int64, error) {
	types := []string{
		"/cosmos.slashing.v1beta1.MsgUnjail",
		"/cosmos.staking.v1beta1.MsgEditValidator",
		"/cosmos.staking.v1beta1.MsgCreateValidator",
	}
	return r.getTransactionsByTypes(ctx, accountAddress, types, limit, offset)
}

func (r *txs) GetPowerEvents(ctx context.Context, accountAddress string, limit int64, offset int64) ([]*model.Tx, int64, error) {
	types := []string{
		"/cosmos.staking.v1beta1.MsgDelegate",
		"/cosmos.staking.v1beta1.MsgUndelegate",
		"/cosmos.staking.v1beta1.MsgBeginRedelegate",
		"/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation",
	}
	return r.getTransactionsByTypes(ctx, accountAddress, types, limit, offset)
}

func (r *txs) getTransactionsByTypes(ctx context.Context, accountAddress string, types []string, limit int64, offset int64) ([]*model.Tx, int64, error) {
	queryEvents := `select txes.hash
		from txes
				 left join blocks on txes.block_id = blocks.id
				 left join messages on txes.id = messages.tx_id
				 left join message_types on messages.message_type_id = message_types.id
				 left join message_events on messages.id = message_events.message_id
				 left join message_event_types on message_events.message_event_type_id=message_event_types.id
				 left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
				 left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
		where message_types.message_type=ANY($4)
		  and message_event_attributes.value=$1
		group by txes.id, txes.timestamp, txes.hash, blocks.height
		order by txes.timestamp desc limit $2 offset $3`
	rows, err := r.db.Query(ctx, queryEvents, accountAddress, limit, offset, types)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	data := make([]*model.Tx, 0)
	for rows.Next() {
		var txHash string
		if err = rows.Scan(&txHash); err != nil {
			return nil, 0, err
		}
		txByHash, _, err := r.Transactions(ctx, 100, 0, &TxsFilter{TxHash: &txHash})
		if err != nil {
			return nil, 0, err
		}
		data = append(data, txByHash...)
	}

	queryTotal := `
		select
			count(txes.hash)
		from txes
				 left join blocks on txes.block_id = blocks.id
				 left join messages on txes.id = messages.tx_id
				 left join message_types on messages.message_type_id = message_types.id
				 left join message_events on messages.id = message_events.message_id
				 left join message_event_types on message_events.message_event_type_id=message_event_types.id
				 left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
				 left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
		where message_types.message_type=ANY($2)
		  and message_event_attributes.value=$1`
	var total int64
	if err = r.db.QueryRow(ctx, queryTotal, accountAddress, types).Scan(&total); err != nil {
		return nil, 0, err
	}

	return data, total, nil
}

func (r *txs) transactionsByEventValuePrepare(values []string, messageType []string,
	limit int64, offset int64,
) (string, []any) {
	params := 4
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+params+1)
	}
	inClause := strings.Join(placeholders, ", ")

	query := fmt.Sprintf(`
		SELECT DISTINCT txes.hash, txes.timestamp
		FROM txes
		LEFT JOIN messages ON txes.id = messages.tx_id
		LEFT JOIN message_types ON messages.message_type_id = message_types.id
		LEFT JOIN message_events ON messages.id = message_events.message_id
		LEFT JOIN message_event_attributes ON message_events.id = message_event_attributes.message_event_id
		WHERE message_types.message_type = ANY($1)
		AND message_event_attributes.value IN (%s)
		GROUP BY txes.id, txes.hash, txes.timestamp
		HAVING COUNT(DISTINCT message_event_attributes.value) = $2::integer
		ORDER BY txes.timestamp DESC
		LIMIT $3::integer OFFSET $4::integer;`, inClause)

	args := make([]interface{}, len(values)+params)
	args[0] = messageType
	args[1] = len(values)
	args[2] = limit
	args[3] = offset
	for i, v := range values {
		args[i+params] = v
	}

	return query, args
}

func (r *txs) transactionsByEventValuePrepareV2(values []string, messageType []string,
	limit int64, offset int64,
) (string, []any) {
	params := 4
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+params+1)
	}
	inClause := strings.Join(placeholders, ", ")

	query := fmt.Sprintf(`
		SELECT DISTINCT txes.tx_hash, txes.tx_timestamp
		FROM tx_events_vals_aggregateds as txes
		WHERE txes.msg_type = ANY($1)
		AND txes.ev_attr_value IN (%s)
		GROUP BY txes.id, txes.tx_hash, txes.tx_timestamp
		HAVING COUNT(DISTINCT txes.ev_attr_value) = $2::integer
		ORDER BY txes.tx_timestamp DESC
		LIMIT $3::integer OFFSET $4::integer;`, inClause)

	args := make([]interface{}, len(values)+params)
	args[0] = messageType
	args[1] = len(values)
	args[2] = limit
	args[3] = offset
	for i, v := range values {
		args[i+params] = fmt.Sprintf("%x", md5.Sum([]byte(strings.ToLower(v))))
	}

	return query, args
}

func (r *txs) transactionsByEventValueTotals(ctx context.Context,
	values []string, messageType []string,
) (int64, error) {
	params := 2
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+params+1)
	}
	inClause := strings.Join(placeholders, ", ")

	queryAll := fmt.Sprintf(`
			SELECT COUNT(DISTINCT txes.tx_hash)
			FROM tx_events_vals_aggregateds as txes
			WHERE txes.msg_type = ANY($1)
			AND txes.ev_attr_value IN (%s)
			HAVING COUNT(DISTINCT txes.ev_attr_value) = $2::integer`, inClause)

	args := make([]interface{}, len(values)+params)
	args[0] = messageType
	args[1] = len(values)
	for i, v := range values {
		args[i+params] = fmt.Sprintf("%x", md5.Sum([]byte(strings.ToLower(v))))
	}

	var total int64
	if err := r.db.QueryRow(ctx, queryAll, args...).Scan(&total); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}

	return total, nil
}

func (r *txs) TransactionsByEventValue(ctx context.Context, values []string, messageType []string, includeEvents bool,
	limit int64, offset int64,
) ([]*model.Tx, int64, error) {
	startTime := time.Now()
	// log.Info().Msgf("=> start TransactionsByEventValue %s", startTime.String())

	query, args := r.transactionsByEventValuePrepareV2(values, messageType, limit, offset)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, err
	}
	defer rows.Close()
	// log.Info().Msgf("=> TransactionsByEventValue done in: %s", time.Since(startTime).String())

	txHashes := make([]string, 0)
	for rows.Next() {
		var txHash string
		var txTime time.Time
		if err = rows.Scan(&txHash, &txTime); err != nil {
			log.Err(err).Msgf("error scanning row")
			continue
		}
		txHashes = append(txHashes, txHash)
	}

	if len(txHashes) == 0 {
		return nil, 0, nil
	}

	data, _, err := r.Transactions(ctx, limit, 0, &TxsFilter{TxHashes: txHashes})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, err
	}

	// log.Info().Msgf("=> TransactionsByEventValue before loop in: %s", time.Since(startTime).String())

	for _, tx := range data {
		if includeEvents {
			events, err := r.GetEventsV2(ctx, tx.Hash)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, 0, err
			}
			tx.Events = events
		}
	}

	// log.Info().Msgf("=> TransactionsByEventValue after loop in: %s", time.Since(startTime).String())

	// calculating total count
	total, err := r.transactionsByEventValueTotals(ctx, values, messageType)
	if err != nil {
		return nil, 0, err
	}

	log.Info().Msgf("=> TransactionsByEventValue after totals in: %s", time.Since(startTime).String())

	return data, total, nil
}

func (r *txs) GetEvents(ctx context.Context, txID uint) ([]*model.TxEvents, error) {
	query := `select
       message_types.message_type,
       COALESCE(message_events.index, 0),
       message_event_types.type,
       COALESCE(message_event_attributes.index, 0),
       message_event_attributes.value,
       message_event_attribute_keys.key
from txes
         left join messages on txes.id = messages.tx_id
         left join message_types on messages.message_type_id = message_types.id
         left join message_events on messages.id = message_events.message_id
         left join message_event_types on message_events.message_event_type_id=message_event_types.id
         left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
         left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
where txes.id=$1
order by messages.message_index, message_events.index, message_event_attributes.index`
	rows, err := r.db.Query(ctx, query, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]*model.TxEvents, 0)
	for rows.Next() {
		var event model.TxEvents
		if err = rows.Scan(&event.MessageType, &event.EventIndex, &event.Type, &event.Index, &event.Value, &event.Key); err != nil {
			// log.Err(err).Msgf("error scanning row in GetEvents, ignoring") TODO
			continue
		}
		data = append(data, &event)
	}
	return data, nil
}

func (r *txs) GetEventsV2(ctx context.Context, txHash string) ([]*model.TxEvents, error) {
	query := `select
       txes.message_type,
       COALESCE(txes.message_type_index, 0),
       COALESCE(txes.message_event_type, ''),
       COALESCE(txes.message_event_attr_index, 0),
       COALESCE(txes.message_event_attr_value, ''),
       COALESCE(txes.message_event_attr_key, '')
from public.tx_events_aggregateds as txes
where txes.tx_hash=$1
order by txes.message_type_index, txes.message_event_attr_index`
	rows, err := r.db.Query(ctx, query, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]*model.TxEvents, 0)
	for rows.Next() {
		var event model.TxEvents
		if err = rows.Scan(&event.MessageType, &event.EventIndex, &event.Type, &event.Index, &event.Value, &event.Key); err != nil {
			log.Err(err).Msgf("error scanning row in GetEventsV2, ignoring")
			continue
		}
		data = append(data, &event)
	}
	return data, nil
}

func (r *txs) GetVotes(ctx context.Context, accountAddress string, uniqueProposals bool, limit int64, offset int64) ([]*model.VotesTransaction, int64, error) {
	voterQuery := `SELECT timestamp, hash, height, voter, proposal_id, option, weight 
					FROM votes_normalized 
					WHERE voter=$1 
					ORDER BY timestamp DESC 
					LIMIT $2 OFFSET $3;`

	if uniqueProposals {
		voterQuery = `SELECT DISTINCT ON (proposal_id) timestamp, hash, height, voter, proposal_id, option, weight 
               FROM votes_normalized 
               WHERE voter=$1 
               ORDER BY proposal_id, timestamp DESC 
               LIMIT $2 OFFSET $3;`
	}

	rows, err := r.db.Query(ctx, voterQuery, accountAddress, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	data := make([]*model.VotesTransaction, 0)
	for rows.Next() {
		var voteTx model.VotesTransaction
		var proposalID string
		if err = rows.Scan(&voteTx.Timestamp, &voteTx.TxHash, &voteTx.BlockHeight,
			&voteTx.Voter, &proposalID, &voteTx.Option, &voteTx.Weight); err != nil {
			return nil, 0, err
		}

		proposal, err := strconv.Atoi(proposalID)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid proposal ID: %s", proposalID)
		}
		voteTx.ProposalID = proposal

		txByHash, _, err := r.Transactions(ctx, 1, 0, &TxsFilter{TxHash: &voteTx.TxHash})
		if err != nil {
			return nil, 0, err
		}
		if len(txByHash) > 0 {
			voteTx.Tx = txByHash[0]
		}

		data = append(data, &voteTx)
	}

	countQuery := `SELECT COUNT(*) FROM votes_normalized WHERE voter=$1;`
	if uniqueProposals {
		countQuery = `SELECT COUNT(DISTINCT proposal_id) FROM votes_normalized WHERE voter=$1;`
	}
	var total int64
	if err = r.db.QueryRow(ctx, countQuery, accountAddress).Scan(&total); err != nil {
		return nil, 0, err
	}

	return data, total, nil
}

func (r *txs) GetVotesByAccounts(ctx context.Context, accounts []string, excludeAccounts bool, voteOption string,
	proposalID int, byAccAddress *string, limit int64, offset int64, sortBy *model.SortBy,
) ([]*model.VotesTransaction, int64, error) {
	dialect := goqu.Select(
		"vn.hash",
		"vn.weight",
		"vn.height",
		"vn.timestamp",
		"vn.option",
		"vn.voter").
		From(goqu.T("votes_normalized").As("vn"))
	if len(accounts) > 0 {
		if excludeAccounts {
			dialect = dialect.Where(goqu.I("vn.voter").NotIn(accounts))
		} else {
			dialect = dialect.Where(goqu.I("vn.voter").In(accounts))
		}
	}
	dialect = dialect.
		Where(goqu.I("vn.option").Eq(voteOption)).
		Where(goqu.I("vn.proposal_id").Eq(fmt.Sprintf("%d", proposalID)))

	if byAccAddress != nil && *byAccAddress != "" {
		dialect = dialect.Where(goqu.I("vn.voter").Eq(byAccAddress))
	}
	if sortBy != nil && sortBy.By != "" {
		if strings.EqualFold(sortBy.Direction, "asc") {
			dialect = dialect.Order(goqu.I(fmt.Sprintf("vn.%s", sortBy.By)).Asc())
		} else {
			dialect = dialect.Order(goqu.I(fmt.Sprintf("vn.%s", sortBy.By)).Desc())
		}
	} else {
		dialect = dialect.Order(goqu.I("vn.timestamp").Desc())
	}
	dialect = dialect.Limit(uint(limit)).Offset(uint(offset))

	query, args, err := dialect.ToSQL()
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		log.Err(err).Msgf("error getting votes by accounts")
		return nil, 0, err
	}
	defer rows.Close()

	data := make([]*model.VotesTransaction, 0)
	for rows.Next() {
		var vote model.VotesTransaction
		if err = rows.Scan(&vote.TxHash, &vote.Weight, &vote.BlockHeight, &vote.Timestamp, &vote.Option, &vote.Voter); err != nil {
			return nil, 0, err
		}
		vote.ProposalID = proposalID

		data = append(data, &vote)
	}

	queryAll, args, err := dialect.
		ClearSelect().
		ClearLimit().
		ClearOffset().
		ClearOrder().
		SelectDistinct(goqu.COUNT("vn.hash")).ToSQL()
	if err != nil {
		return nil, 0, err
	}
	row := r.db.QueryRow(ctx, queryAll, args...)
	var all int64
	if err = row.Scan(&all); err != nil {
		log.Err(err).Msgf("error getting total amount")
		return nil, 0, err
	}
	return data, all, nil
}

func (r *txs) UpdateViews(ctx context.Context) error {
	_, err := r.db.Exec(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY transactions_normalized WITH DATA;`)
	if err != nil {
		log.Err(err).Msgf("error refreshing transactions_normalized")
		return err
	}

	_, err = r.db.Exec(ctx, `
		REFRESH MATERIALIZED VIEW CONCURRENTLY votes_normalized WITH DATA;
	`)
	if err != nil {
		log.Err(err).Msgf("error refreshing votes_normalized")
		return err
	}

	_, err = r.db.Exec(ctx, `
		REFRESH MATERIALIZED VIEW CONCURRENTLY depositors_normalized WITH DATA;
	`)
	if err != nil {
		log.Err(err).Msgf("error refreshing depositors_normalized")
		return err
	}

	return err
}

func (r *txs) AccountInfo(ctx context.Context, account string) (*model.AccountInfo, error) {
	query := `SELECT
    COUNT(DISTINCT txs.tx_hash) as tx_count,
    MIN(txs.time) as first_tx,
    SUM(CASE WHEN CAST(txs.tx_type AS TEXT)='sender' THEN  CAST(txs.amount AS BIGINT) ELSE 0 END) as total_spent,
    SUM(CASE WHEN CAST(txs.tx_type AS TEXT)='receiver' THEN CAST(txs.amount AS BIGINT) ELSE 0 END) as total_received,
    txs.denom
from transactions_normalized txs where account = $1
group by txs.denom;`
	var acc model.AccountInfo
	var totalReceived model.DecCoin
	var totalSpent model.DecCoin
	var denom string

	err := r.db.QueryRow(ctx, query, account).Scan(&acc.TotalTransactions,
		&acc.FirstTransactionDate, &totalSpent.Amount, &totalReceived.Amount, &denom)
	if err != nil {
		log.Err(err).Msgf("failed to fetch votes for account: %v", account)
		return nil, err
	}
	totalReceived.Denom = denom
	totalSpent.Denom = denom
	acc.TotalSpent = totalSpent
	acc.TotalReceived = totalReceived

	return &acc, nil
}

func (r *txs) DelegatesByValidator(ctx context.Context, from, to time.Time, valoperAddress string,
	limit int64, offset int64,
) (data []*model.Tx, totalSum *model.Denom, all int64, err error) {
	query := `SELECT hash from tx_delegate_aggregateds 
            where date(timestamp) BETWEEN date($1) and date($2) and validator=$3 
            LIMIT $4::integer OFFSET $5::integer;`

	rows, err := r.db.Query(ctx, query, from.UTC(), to.UTC(), valoperAddress, limit, offset)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var txHash string
		if err = rows.Scan(&txHash); err != nil {
			return nil, nil, 0, err
		}

		txByHash, _, err := r.Transactions(ctx, 100, 0, &TxsFilter{TxHash: &txHash})
		if err != nil {
			return nil, nil, 0, err
		}

		for _, tx := range txByHash {
			events, err := r.GetEvents(ctx, tx.ID)
			if err != nil {
				log.Err(err).Msgf("can't query events for tx: %v", tx.ID)
				continue
			}
			tx.Events = events
		}
		data = append(data, txByHash...)
	}

	queryTotal := `SELECT sum(amount), denom, count(hash) from tx_delegate_aggregateds 
            where date(timestamp) BETWEEN date($1) and date($2) and validator=$3 GROUP BY denom;`

	var totalDec decimal.Decimal
	var totalRes model.Denom
	row := r.db.QueryRow(ctx, queryTotal, from.UTC(), to.UTC(), valoperAddress)
	if err = row.Scan(&totalDec, &totalRes.Denom, &all); err != nil {
		return nil, nil, 0, err
	}
	totalRes.Amount = totalDec.String()

	return data, &totalRes, all, nil
}

func (r *txs) ProposalDepositors(ctx context.Context, proposalID int,
	sortBy *model.SortBy, limit int64, offset int64,
) ([]*model.ProposalDeposit, int64, error) {
	dialect := goqu.Select(
		"dp.hash",
		"dp.timestamp",
		"dp.sender",
		"dp.amount",
		"dp.denom").
		From(goqu.T("depositors_normalized").As("dp"))
	dialect = dialect.
		Where(goqu.I("dp.proposal_id").Eq(fmt.Sprintf("%d", proposalID)))

	if sortBy != nil && sortBy.By != "" {
		if strings.EqualFold(sortBy.Direction, "asc") {
			dialect = dialect.Order(goqu.I(fmt.Sprintf("dp.%s", sortBy.By)).Asc())
		} else {
			dialect = dialect.Order(goqu.I(fmt.Sprintf("dp.%s", sortBy.By)).Desc())
		}
	} else {
		dialect = dialect.Order(goqu.I("dp.timestamp").Desc())
	}
	dialect = dialect.Limit(uint(limit)).Offset(uint(offset))

	query, args, err := dialect.ToSQL()
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		log.Err(err).Msgf("error getting votes by accounts")
		return nil, 0, err
	}
	defer rows.Close()

	data := make([]*model.ProposalDeposit, 0)
	for rows.Next() {
		var deposit model.ProposalDeposit
		var amount model.DecCoin
		if err = rows.Scan(&deposit.TxHash, &deposit.TxTime, &deposit.Address, &amount.Amount, &amount.Denom); err != nil {
			return nil, 0, err
		}
		deposit.Amount = amount

		data = append(data, &deposit)
	}

	queryAll, args, err := dialect.
		ClearSelect().
		ClearLimit().
		ClearOffset().
		ClearOrder().
		SelectDistinct(goqu.COUNT("dp.hash")).ToSQL()
	if err != nil {
		return nil, 0, err
	}
	row := r.db.QueryRow(ctx, queryAll, args...)
	var all int64
	if err = row.Scan(&all); err != nil {
		log.Err(err).Msgf("error getting total amount")
		return nil, 0, err
	}

	return data, all, nil
}

func (r *txs) TotalRewardByAccount(ctx context.Context, account string) ([]*model.DecCoin, error) {
	query := `SELECT
			SUBSTRING(tx_events_aggregateds.message_event_attr_value FROM '[a-zA-Z]+') AS denom,
			SUM(SUBSTRING(tx_events_aggregateds.message_event_attr_value FROM '[0-9]+')::INTEGER) AS amount
		FROM tx_events_aggregateds
		WHERE tx_events_aggregateds.tx_hash IN (
			SELECT DISTINCT tx_events_aggregateds.tx_hash
			FROM tx_events_aggregateds
			WHERE tx_events_aggregateds.message_type = '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
			AND tx_events_aggregateds.message_event_type = 'transfer'
			AND tx_events_aggregateds.message_event_attr_key = 'recipient'
			AND tx_events_aggregateds.message_event_attr_value = $1
			) AND tx_events_aggregateds.message_event_attr_key = 'amount'
		GROUP BY denom;`
	rows, err := r.db.Query(ctx, query, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]*model.DecCoin, 0)
	for rows.Next() {
		var denom model.DecCoin
		var amount int64
		if err = rows.Scan(&denom.Denom, &amount); err != nil {
			return nil, err
		}
		denom.Amount = decimal.NewFromInt(amount)
		data = append(data, &denom)
	}
	return data, nil
}
