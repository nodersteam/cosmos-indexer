package service

import (
	"context"
	"time"

	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
)

type Blocks interface {
	BlockInfo(ctx context.Context, block int32) (*model.BlockInfo, error)
	BlockInfoByHash(ctx context.Context, hash string) (*model.BlockInfo, error)
	BlockValidators(ctx context.Context, block int32) ([]string, error)
	TotalBlocks(ctx context.Context, to time.Time) (*model.TotalBlocks, error)
	LatestBlockHeight(ctx context.Context) (int64, error)
	Blocks(ctx context.Context, limit int64, offset int64) ([]*model.BlockInfo, int64, error)
	BlockSignatures(ctx context.Context, height int64, valAddress []string,
		limit int64, offset int64) ([]*model.BlockSigners, int64, error)
	BlockUptime(ctx context.Context, blockWindow, height int64,
		validatorAddr string) (float32, error)
	UptimeByBlocks(ctx context.Context, blockWindow, height int64,
		validatorAddr string) ([]*model.BlockSigned, float32, error)
}

type blocks struct {
	blocksRepo repository.Blocks
}

func NewBlocks(blocksRepo repository.Blocks) Blocks {
	return &blocks{blocksRepo: blocksRepo}
}

func (s *blocks) BlockInfo(ctx context.Context, block int32) (*model.BlockInfo, error) {
	return s.blocksRepo.GetBlockInfo(ctx, block)
}

func (s *blocks) BlockInfoByHash(ctx context.Context, hash string) (*model.BlockInfo, error) {
	return s.blocksRepo.GetBlockInfoByHash(ctx, hash)
}

func (s *blocks) BlockValidators(ctx context.Context, block int32) ([]string, error) {
	return s.blocksRepo.GetBlockValidators(ctx, block)
}

func (s *blocks) TotalBlocks(ctx context.Context, to time.Time) (*model.TotalBlocks, error) {
	return s.blocksRepo.TotalBlocks(ctx, to)
}

func (s *blocks) LatestBlockHeight(ctx context.Context) (int64, error) {
	return s.blocksRepo.LatestBlockHeight(ctx)
}

func (s *blocks) Blocks(ctx context.Context, limit int64, offset int64) ([]*model.BlockInfo, int64, error) {
	return s.blocksRepo.Blocks(ctx, limit, offset)
}

func (s *blocks) BlockSignatures(ctx context.Context, height int64, valAddress []string, limit int64, offset int64) ([]*model.BlockSigners, int64, error) {
	return s.blocksRepo.BlockSignatures(ctx, height, valAddress, limit, offset)
}

func (s *blocks) BlockUptime(ctx context.Context, blockWindow, height int64, validatorAddr string) (float32, error) {
	return s.blocksRepo.BlockUptime(ctx, blockWindow, height, validatorAddr)
}

func (s *blocks) UptimeByBlocks(ctx context.Context, blockWindow, height int64, validatorAddr string) ([]*model.BlockSigned, float32, error) {
	return s.blocksRepo.UptimeByBlocks(ctx, blockWindow, height, validatorAddr)
}
