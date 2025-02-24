package consumer

import (
	"context"
	"time"

	"github.com/noders-team/cosmos-indexer/pkg/service"

	"github.com/noders-team/cosmos-indexer/pkg/repository"
	"github.com/rs/zerolog/log"
)

type AggregatesConsumer interface {
	Consume(ctx context.Context) error
	RefreshMaterializedViews(ctx context.Context) error
}

type aggregatesConsumer struct {
	txs           repository.Txs
	srvAggregates service.Aggregates
}

func NewAggregatesConsumer(txs repository.Txs, srvAggregates service.Aggregates) AggregatesConsumer {
	return &aggregatesConsumer{txs: txs, srvAggregates: srvAggregates}
}

func (s *aggregatesConsumer) Consume(ctx context.Context) error {
	log.Info().Msg("starting aggregates consumer")
	t := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			ctxTimeout, done := context.WithTimeout(ctx, 50*time.Second)
			defer done()
			_, err := s.srvAggregates.StoreAggregates(ctxTimeout)
			if err != nil {
				log.Error().Err(err).Msg("failed to store aggregated data in consumer")
			}
		}
	}
}

func (s *aggregatesConsumer) RefreshMaterializedViews(ctx context.Context) error {
	log.Info().Msgf("RefreshMaterializedViews started %s", time.Now().String())
	t := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.txs.UpdateViews(ctx); err != nil {
				log.Err(err).Msg("failed to update views")
			}
			log.Info().Msgf("RefreshMaterializedViews finished %s", time.Now().String())
		}
	}
}
