package service

import (
	"context"
	"fmt"
	"time"

	"github.com/nodersteam/cosmos-indexer/db/models"
	"github.com/rs/zerolog/log"

	"github.com/nodersteam/cosmos-indexer/pkg/model"
	"github.com/nodersteam/cosmos-indexer/pkg/repository"
)

type Txs interface {
	ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error)
	GetTxByHash(ctx context.Context, txHash string) (*models.Tx, error)
	TotalTransactions(ctx context.Context, to time.Time) (*model.TotalTransactions, error)
	Transactions(ctx context.Context, offset int64, limit int64) ([]*models.Tx,
		int64, error)
	TransactionsByBlock(ctx context.Context, height int64, offset int64,
		limit int64) ([]*models.Tx, int64, error)
	TransactionRawLog(ctx context.Context, hash string) ([]byte, error)
	TransactionSigners(ctx context.Context, hash string) ([]*models.SignerInfo, error)
	Messages(ctx context.Context, hash string) ([]*models.Message, error)
	GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error)
	ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error)
	ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error)
}

type txs struct {
	txRepo repository.Txs
}

func NewTxs(txRepo repository.Txs) *txs {
	return &txs{txRepo: txRepo}
}

func (s *txs) ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error) {
	return s.txRepo.ChartTxByDay(ctx, from, to)
}

func (s *txs) TransactionRawLog(ctx context.Context, hash string) ([]byte, error) {
	return s.txRepo.TransactionRawLog(ctx, hash)
}

func (s *txs) Transactions(ctx context.Context, offset int64, limit int64) ([]*models.Tx, int64, error) {
	transactions, all, err := s.txRepo.Transactions(ctx, limit, offset, nil)
	log.Debug().Msgf("transactions len %d", len(transactions))
	if err != nil {
		return nil, 0, err
	}
	return transactions, all, nil
}

func (s *txs) TotalTransactions(ctx context.Context, to time.Time) (*model.TotalTransactions, error) {
	var res model.TotalTransactions
	var err error
	res.Total, res.Total24H, res.Total48H, res.Total30D, err = s.txRepo.TransactionsPerPeriod(ctx, to)
	if err != nil {
		return nil, err
	}

	res.Volume24H, res.Volume30D, err = s.txRepo.VolumePerPeriod(ctx, to)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func (s *txs) GetTxByHash(ctx context.Context, txHash string) (*models.Tx, error) {
	transactions, _, err := s.txRepo.Transactions(ctx, 10, 0, &repository.TxsFilter{TxHash: &txHash})
	log.Debug().Msgf("transactions len %d", len(transactions))
	if err != nil {
		return nil, err
	}
	if len(transactions) == 0 {
		return nil, fmt.Errorf("not found")
	}
	txRes := transactions[0]
	return txRes, nil
}

func (s *txs) TransactionsByBlock(ctx context.Context, height int64, limit int64, offset int64) ([]*models.Tx, int64, error) {
	transactions, all, err := s.txRepo.Transactions(ctx, limit, offset, &repository.TxsFilter{TxBlockHeight: &height})
	log.Debug().Msgf("transactions len %d", len(transactions))
	if err != nil {
		return nil, 0, err
	}

	return transactions, all, nil
}

func (s *txs) TransactionSigners(ctx context.Context, hash string) ([]*models.SignerInfo, error) {
	return s.txRepo.TransactionSigners(ctx, hash)
}

func (s *txs) Messages(ctx context.Context, hash string) ([]*models.Message, error) {
	return s.txRepo.Messages(ctx, hash)
}

func (s *txs) GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error) {
	return s.txRepo.GetSenderAndReceiver(ctx, hash)
}

func (s *txs) ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error) {
	return s.txRepo.ChartTransactionsByHour(ctx, to)
}

func (s *txs) ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error) {
	return s.txRepo.ChartTransactionsVolume(ctx, to)
}
