package repository

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/shopspring/decimal"

	goqu "github.com/doug-martin/goqu/v9"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noders-team/cosmos-indexer/pkg/model"
)

type Blocks interface {
	GetBlockInfo(ctx context.Context, block int32) (*model.BlockInfo, error)
	GetBlockInfoByHash(ctx context.Context, hash string) (*model.BlockInfo, error)
	GetBlockValidators(ctx context.Context, block int32) ([]string, error)
	TotalBlocks(ctx context.Context, to time.Time) (*model.TotalBlocks, error)
	Blocks(ctx context.Context, limit int64, offset int64) ([]*model.BlockInfo, int64, error)
	LatestBlockHeight(ctx context.Context) (int64, error)
	BlockSignatures(ctx context.Context, height int64, valAddress []string,
		limit int64, offset int64) ([]*model.BlockSigners, int64, error)
	BlockUptime(ctx context.Context, blockWindow, height int64,
		validatorAddr string) (float32, error)
	UptimeByBlocks(ctx context.Context, blockWindow, height int64,
		validatorAddr string) ([]*model.BlockSigned, float32, error)
}

type blocks struct {
	db *pgxpool.Pool
}

func NewBlocks(db *pgxpool.Pool) Blocks {
	return &blocks{db: db}
}

func (r *blocks) BlockUptime(ctx context.Context, blockWindow, height int64, validatorAddr string) (float32, error) {
	query := `select count(b.height)
				from block_signatures bs 
    			left join public.blocks b on b.id = bs.block_id
    			where b.height between $1 and $2
    			and bs.validator_address = $3`
	var totalSigned int64
	err := r.db.QueryRow(ctx, query, height-blockWindow, height, validatorAddr).Scan(
		&totalSigned)
	if err != nil {
		return 0, fmt.Errorf("exec %v", err)
	}
	upTime := (float32(totalSigned) / float32(blockWindow)) * 100.00
	return upTime, nil
}

func (r *blocks) GetBlockInfo(ctx context.Context, block int32) (*model.BlockInfo, error) {
	query := `
				SELECT bl.id, bl.height, addr.address as proposed_validator, bl.time_stamp, bl.block_hash
				from blocks bl 
				LEFT JOIN addresses addr on bl.proposer_cons_address_id = addr.id
				where bl.height = $1
				`
	o := new(model.BlockInfo)
	var blockID int64
	err := r.db.QueryRow(ctx, query, block).Scan(
		&blockID,
		&o.BlockHeight,
		&o.ProposedValidatorAddress,
		&o.GenerationTime,
		&o.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("exec %v", err)
	}

	o.TotalFees, err = r.blockFees(ctx, o.BlockHeight)
	if err != nil {
		return nil, err
	}

	o.GasUsed, o.GasWanted, err = r.blockGas(ctx, o.BlockHeight)
	if err != nil {
		return nil, err
	}

	allTx, err := r.countAllTxs(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("countAllTxs %v", err)
	}
	o.TotalTx = allTx

	return o, nil
}

func (r *blocks) GetBlockInfoByHash(ctx context.Context, hash string) (*model.BlockInfo, error) {
	query := `
				SELECT bl.id, bl.height, COALESCE(addr.address,'') as proposed_validator, bl.time_stamp, bl.block_hash
				from blocks bl 
				LEFT JOIN addresses addr on bl.proposer_cons_address_id = addr.id
				where bl.block_hash = $1
				`
	o := new(model.BlockInfo)
	var blockID int64
	err := r.db.QueryRow(ctx, query, hash).Scan(
		&blockID,
		&o.BlockHeight,
		&o.ProposedValidatorAddress,
		&o.GenerationTime,
		&o.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("exec %v", err)
	}

	o.TotalFees, err = r.blockFees(ctx, o.BlockHeight)
	if err != nil {
		return nil, fmt.Errorf("exec total fees %v", err)
	}

	o.GasUsed, o.GasWanted, err = r.blockGas(ctx, o.BlockHeight)
	if err != nil {
		return nil, fmt.Errorf("exec total gas %v", err)
	}

	allTx, err := r.countAllTxs(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("countAllTxs %v", err)
	}
	o.TotalTx = allTx

	return o, nil
}

func (r *blocks) countAllTxs(ctx context.Context, blockID int64) (int64, error) {
	queryAll := `select count(*) from txes where txes.block_id = $1`
	row := r.db.QueryRow(ctx, queryAll, blockID)
	var allTx int64
	if err := row.Scan(&allTx); err != nil {
		return 0, fmt.Errorf("row.Scan %v", err)
	}
	return allTx, nil
}

func (r *blocks) GetBlockValidators(ctx context.Context, block int32) ([]string, error) {
	query := `
				SELECT addr.address
				FROM blocks bl
				INNER JOIN txes tx on bl.id = tx.block_id
				INNER JOIN tx_signer_addresses signs on tx.id = signs.tx_id
				INNER JOIN addresses addr on signs.address_id = addr.id
				where bl.height = $1
				`
	rows, err := r.db.Query(ctx, query, block)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := make([]string, 0)

	for rows.Next() {
		var in string
		errScan := rows.Scan(&in)
		if errScan != nil {
			return nil, fmt.Errorf("repository.GetBlockValidators, Scan: %v", errScan)
		}
		data = append(data, in)
	}

	return data, nil
}

func (r *blocks) LatestBlockHeight(ctx context.Context) (int64, error) {
	query := `select blocks.height from blocks order by blocks.height desc limit 1`
	row := r.db.QueryRow(ctx, query)
	var blockHeight int64
	if err := row.Scan(&blockHeight); err != nil {
		return 0, err
	}
	return blockHeight, nil
}

func (r *blocks) TotalBlocks(ctx context.Context, to time.Time) (*model.TotalBlocks, error) {
	query := `SELECT COALESCE(MAX(blocks.height), 0) as height FROM blocks`
	row := r.db.QueryRow(ctx, query)
	var blockHeight int64
	if err := row.Scan(&blockHeight); err != nil {
		return nil, err
	}

	from := to.UTC().Truncate(24 * time.Hour)
	count24H, err := r.blocksCount(ctx, from, to.UTC())
	if err != nil {
		return nil, err
	}

	from48h := from.Add(-24 * time.Hour)
	count48H, err := r.blocksCount(ctx, from48h.Truncate(24*time.Hour), from)
	if err != nil {
		return nil, err
	}

	blockTime, err := r.blockTime(ctx)
	if err != nil {
		return nil, err
	}

	query = `SELECT COALESCE(SUM(fees.amount), 0)
				FROM fees
				INNER JOIN txes ON fees.tx_id = txes.id
				INNER JOIN blocks ON txes.block_id = blocks.id
				WHERE blocks.time_stamp BETWEEN $1 AND $2`
	row = r.db.QueryRow(ctx, query, from, to.UTC())
	var feeSum decimal.Decimal
	if err = row.Scan(&feeSum); err != nil {
		log.Err(err).Msgf("row.Scan(&feeSum)")
		return nil, err
	}

	return &model.TotalBlocks{
		BlockHeight: blockHeight,
		Count24H:    count24H,
		Count48H:    count48H,
		BlockTime:   int64(blockTime),
		TotalFee24H: feeSum,
	}, nil
}

func (r *blocks) blockTime(ctx context.Context) (float64, error) {
	query := `SELECT time_stamp from blocks order by height desc limit 100`
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var times []int64

	var prevBlockTime *time.Time
	for rows.Next() {
		var time time.Time
		if err = rows.Scan(&time); err != nil {
			return 0, err
		}
		if prevBlockTime == nil {
			prevBlockTime = &time
			continue
		}
		dur := prevBlockTime.Sub(time)
		times = append(times, int64(dur.Seconds()))
		prevBlockTime = &time
	}

	return r.calculateMedian(times), nil
}

// CalculateMedian calculates the median of a slice of int64
func (r *blocks) calculateMedian(times []int64) float64 {
	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	n := len(times)
	if n == 0 {
		// Return a default value or handle the empty slice case
		return 0
	}

	if n%2 == 1 {
		// If odd, return the middle element
		return float64(times[n/2])
	}

	// If even, return the average of the two middle elements
	mid1 := times[n/2-1]
	mid2 := times[n/2]
	return float64(mid1+mid2) / 2.0
}

func (r *blocks) blocksCount(ctx context.Context, from, to time.Time) (int64, error) {
	query := `select count(*) from blocks where blocks.time_stamp between $1 AND $2`
	row := r.db.QueryRow(ctx, query, from, to)
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

func (r *blocks) Blocks(ctx context.Context, limit int64, offset int64) ([]*model.BlockInfo, int64, error) {
	query := `select blocks.id, blocks.height, blocks.block_hash, addresses.address as proposer, blocks.time_stamp from blocks
		left join addresses on blocks.proposer_cons_address_id = addresses.id
		group by blocks.id, blocks.height, blocks.block_hash, addresses.address, blocks.time_stamp
		order by blocks.height desc
		limit $1 offset $2`

	rows, err := r.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	data := make([]*model.BlockInfo, 0)
	for rows.Next() {
		var in model.BlockInfo
		blockID := 0
		errScan := rows.Scan(&blockID, &in.BlockHeight,
			&in.BlockHash, &in.ProposedValidatorAddress, &in.GenerationTime)
		if errScan != nil {
			return nil, 0, fmt.Errorf("repository.Blocks, Scan: %v", errScan)
		}

		allTx, err := r.countAllTxs(ctx, int64(blockID))
		if err != nil {
			return nil, 0, fmt.Errorf("rowQueryTxs.Scan, Scan: %v", errScan)
		}
		in.TotalTx = allTx

		data = append(data, &in)
	}

	query = `select count(*) from blocks`
	row := r.db.QueryRow(ctx, query)
	var all int64
	if err = row.Scan(&all); err != nil {
		return nil, 0, err
	}

	return data, all, nil
}

func (r *blocks) blockFees(ctx context.Context, height int64) (decimal.Decimal, error) {
	queryFees := `select blocks.height, sum(COALESCE(fees.amount,0)) from blocks
                 left join txes on blocks.id = txes.block_id
                 left join fees on txes.id = fees.tx_id
                 where blocks.height = $1
				 group by blocks.height`
	rowFees := r.db.QueryRow(ctx, queryFees, height)

	var blockHeight int64
	var totalFees decimal.Decimal

	if err := rowFees.Scan(&blockHeight, &totalFees); err != nil {
		return decimal.Zero, fmt.Errorf("rowFees.Scan, Scan: %v", err)
	}
	return totalFees, nil
}

func (r *blocks) blockGas(ctx context.Context, height int64) (decimal.Decimal, decimal.Decimal, error) {
	queryGas := `select blocks.height, sum(COALESCE(tx_responses.gas_wanted,0)), sum(COALESCE(tx_responses.gas_used,0)) from blocks
						left join txes on blocks.id = txes.block_id
						left join tx_responses on txes.tx_response_id = tx_responses.id
						where blocks.height = $1
						group by blocks.height`
	rowQueryGas := r.db.QueryRow(ctx, queryGas, height)

	var blockHeight int64
	var gasUsed decimal.Decimal
	var gasWanted decimal.Decimal

	if err := rowQueryGas.Scan(&blockHeight, &gasUsed, &gasWanted); err != nil {
		return decimal.Zero, decimal.Zero, fmt.Errorf("rowQueryGas.Scan, Scan: %v", err)
	}

	return gasUsed, gasWanted, nil
}

func (r *blocks) BlockSignatures(ctx context.Context, height int64, valAddress []string, limit int64, offset int64) ([]*model.BlockSigners, int64, error) {
	dialect := goqu.Select("blocks.height", "block_signatures.validator_address", "block_signatures.timestamp").
		From(goqu.T("block_signatures")).
		LeftJoin(
			goqu.T("blocks"),
			goqu.On(goqu.Ex{"block_signatures.block_id": goqu.I("blocks.id")}))

	whereExp := make([]goqu.Expression, 0)
	whereExp = append(whereExp, goqu.C("height").Eq(height))
	if len(valAddress) > 0 {
		whereExp = append(whereExp, goqu.C("validator_address").In(valAddress))
	}
	dialect = dialect.Where(whereExp...)
	dialect = dialect.Limit(uint(limit)).Offset(uint(offset))

	queryLimit, _, err := dialect.ToSQL()
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.db.Query(ctx, queryLimit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	res := make([]*model.BlockSigners, 0)
	for rows.Next() {
		var in model.BlockSigners
		if err := rows.Scan(&in.BlockHeight, &in.Validator, &in.Time); err != nil {
			return nil, 0, err
		}
		res = append(res, &in)
	}

	queryAll, _, err := dialect.
		ClearSelect().
		ClearLimit().
		ClearOffset().
		Select(goqu.COUNT("block_signatures.validator_address")).ToSQL()
	if err != nil {
		return nil, 0, err
	}

	row := r.db.QueryRow(ctx, queryAll)
	var all int64
	if err = row.Scan(&all); err != nil {
		return nil, 0, err
	}

	return res, all, nil
}

func (r *blocks) UptimeByBlocks(ctx context.Context, blockWindow, height int64, validatorAddr string) ([]*model.BlockSigned, float32, error) {
	query := `select b.height
				from block_signatures bs 
    			left join public.blocks b on b.id = bs.block_id
    			where b.height between $1 and $2
    			and bs.validator_address = $3`
	rows, err := r.db.Query(ctx, query, height-blockWindow, height, validatorAddr)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	data := make(map[int64]bool)
	for rows.Next() {
		var signedHeight int64
		if err = rows.Scan(&signedHeight); err != nil {
			return nil, 0, err
		}
		data[signedHeight] = true
	}

	unsigned := make([]*model.BlockSigned, 0)
	init := height - blockWindow
	for i := init; i < height; i++ {
		_, found := data[i]
		if !found {
			unsigned = append(unsigned, &model.BlockSigned{BlockHeight: i, Signed: false})
		}
	}

	upTime := (float32(len(data)) / float32(blockWindow)) * 100.00
	return unsigned, upTime, nil
}
