package consumer

import (
	"context"

	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
	"github.com/rs/zerolog/log"
)

type CacheConsumer interface {
	RunBlocks(ctx context.Context) error
	RunTransactions(ctx context.Context) error
}

type cacheConsumer struct {
	blocksCh chan *model.BlockInfo
	txCh     chan *model.Tx
	blocks   repository.BlocksCache
	txs      repository.TransactionsCache
}

func NewCacheConsumer(blocks repository.BlocksCache, blocksCh chan *model.BlockInfo,
	txCh chan *model.Tx, txs repository.TransactionsCache,
) CacheConsumer {
	return &cacheConsumer{blocks: blocks, blocksCh: blocksCh, txCh: txCh, txs: txs}
}

func (s *cacheConsumer) RunBlocks(ctx context.Context) error {
	log.Info().Msgf("Starting cache consumer: RunBlocks")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("breaking the worker loop.")
			break
		case newBlock := <-s.blocksCh:
			err := s.blocks.AddBlock(ctx, newBlock)
			if err != nil {
				log.Err(err).Msgf("Error publishing block")
			}
		}
	}
}

func (s *cacheConsumer) RunTransactions(ctx context.Context) error {
	log.Info().Msgf("Starting cache consumer: RunTransactions")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("breaking the worker loop.")
			break
		case newTx := <-s.txCh:
			err := s.txs.AddTransaction(ctx, newTx)
			if err != nil {
				log.Err(err).Msgf("Error publishing block")
			}
		}
	}
}
