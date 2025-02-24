package service

import (
	"context"
	"time"

	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
	"github.com/rs/zerolog/log"
)

type Aggregates interface {
	StoreAggregates(ctx context.Context) (*model.AggregatedInfo, error)
	GetTotals(ctx context.Context) (*model.AggregatedInfo, error)
}

type aggregates struct {
	totals repository.TotalsCache
	blocks repository.Blocks
	txs    repository.Txs
}

func NewAggregates(totals repository.TotalsCache, blocks repository.Blocks, txs repository.Txs) Aggregates {
	return &aggregates{totals: totals, blocks: blocks, txs: txs}
}

func (s *aggregates) GetTotals(ctx context.Context) (*model.AggregatedInfo, error) {
	info, err := s.totals.GetTotals(ctx)
	if err != nil {
		log.Warn().Msgf("failed to get totals from cache, requesting new %v", err)
		ctxTimeout, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		info, err = s.StoreAggregates(ctxTimeout)
		if err != nil {
			log.Err(err).Msg("failed to store aggregated data in service")
			return nil, err
		}
	}
	return info, nil
}

func (s *aggregates) StoreAggregates(ctx context.Context) (*model.AggregatedInfo, error) {
	log.Info().Msg("starting to store aggregates")

	blocksTotal, err := s.blocks.TotalBlocks(ctx, time.Now().UTC())
	if err != nil {
		log.Err(err).Msg("failed to fetch total blocks")
		return nil, err
	}

	var res model.TotalTransactions
	res.Total, res.Total24H, res.Total48H, res.Total30D, err = s.txs.TransactionsPerPeriod(ctx, time.Now().UTC())
	if err != nil {
		log.Err(err).Msg("failed to fetch transactions per period")
		return nil, err
	}

	res.Volume24H, res.Volume30D, err = s.txs.VolumePerPeriod(ctx, time.Now().UTC())
	if err != nil {
		log.Err(err).Msg("failed to fetch transactions volume per period")
		return nil, err
	}

	wallets, err := s.txs.GetWalletsCount(ctx)
	if err != nil {
		log.Err(err).Msg("failed to fetch GetWalletsCount")
		return nil, err
	}

	info := &model.AggregatedInfo{
		UpdatedAt:    time.Now().UTC(),
		Blocks:       *blocksTotal,
		Transactions: res,
		Wallets:      *wallets,
	}

	err = s.totals.AddTotals(ctx, info)
	if err != nil {
		log.Err(err).Msg("failed to add totals to cache")
		return nil, err
	}
	log.Info().Msg("finished to store aggregates")

	return info, nil
}
