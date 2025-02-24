package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/rs/zerolog/log"

	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
)

type Txs interface {
	ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error)
	GetTxByHash(ctx context.Context, txHash string) (*model.Tx, error)
	TotalTransactions(ctx context.Context, to time.Time) (*model.TotalTransactions, error)
	Transactions(ctx context.Context, offset int64, limit int64) ([]*model.Tx,
		int64, error)
	TransactionsByBlock(ctx context.Context, height int64, offset int64,
		limit int64) ([]*model.Tx, int64, error)
	TransactionRawLog(ctx context.Context, hash string) ([]byte, error)
	TransactionSigners(ctx context.Context, hash string) ([]*model.SignerInfo, error)
	Messages(ctx context.Context, hash string) ([]*model.Message, error)
	GetSenderAndReceiver(ctx context.Context, hash string) (*model.TxSenderReceiver, error)
	ChartTransactionsByHour(ctx context.Context, to time.Time) (*model.TxByHourWithCount, error)
	ChartTransactionsVolume(ctx context.Context, to time.Time) ([]*model.TxVolumeByHour, error)
	GetVotes(ctx context.Context, accountAddress string, uniqueProposals bool, limit int64, offset int64) ([]*model.VotesTransaction, int64, error)
	GetPowerEvents(ctx context.Context, accountAddress string,
		limit int64, offset int64) ([]*model.Tx, int64, error)
	GetValidatorHistoryEvents(ctx context.Context, accountAddress string,
		limit int64, offset int64) ([]*model.Tx, int64, error)
	TransactionsByEventValue(ctx context.Context, values []string,
		messageType []string, limit int64, offset int64) ([]*model.Tx, int64, error)
	GetVotesByAccounts(ctx context.Context, accounts []string, excludeAcc bool, voteType string,
		proposalID int, byAccAddress *string, limit int64, offset int64, sortBy *model.SortBy) ([]*model.VotesTransaction, int64, error)
	GetWalletsCountPerPeriod(ctx context.Context, startDate, endDate time.Time) (int64, error)
	GetWalletsWithTx(ctx context.Context, limit int64, offset int64) ([]*model.WalletWithTxs, int64, error)
	TxCountByAccounts(ctx context.Context, accounts []string) ([]*model.WalletWithTxs, error)
	AccountInfo(ctx context.Context, account string) (*model.AccountInfo, error)
	DelegatesByValidator(ctx context.Context, from, to time.Time, valoperAddress string,
		limit int64, offset int64) (data []*model.Tx, totalSum *model.Denom, all int64, err error)
	ProposalDepositors(ctx context.Context, proposalID int,
		sortBy *model.SortBy, limit int64, offset int64) ([]*model.ProposalDeposit, int64, error)
	TotalRewardByAccount(ctx context.Context, account string) ([]*model.DecCoin, error)
}

type txs struct {
	txRepo repository.Txs
}

func NewTxs(txRepo repository.Txs) Txs {
	return &txs{txRepo: txRepo}
}

func (s *txs) ChartTxByDay(ctx context.Context, from time.Time, to time.Time) ([]*model.TxsByDay, error) {
	return s.txRepo.ChartTxByDay(ctx, from, to)
}

func (s *txs) TransactionRawLog(ctx context.Context, hash string) ([]byte, error) {
	return s.txRepo.TransactionRawLog(ctx, hash)
}

func (s *txs) Transactions(ctx context.Context, offset int64, limit int64) ([]*model.Tx, int64, error) {
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

func (s *txs) GetTxByHash(ctx context.Context, txHash string) (*model.Tx, error) {
	transactions, _, err := s.txRepo.Transactions(ctx, 10, 0, &repository.TxsFilter{TxHash: &txHash})
	log.Debug().Msgf("transactions len %d", len(transactions))
	if err != nil {
		return nil, err
	}
	if len(transactions) == 0 {
		return nil, fmt.Errorf("not found")
	}

	for _, tx := range transactions {
		events, err := s.txRepo.GetEvents(ctx, tx.ID)
		if err != nil {
			log.Err(err).Msgf("error getting events for tx %d", tx.ID)
			continue
		}
		tx.Events = events
	}
	txRes := transactions[0]
	return txRes, nil
}

func (s *txs) TransactionsByBlock(ctx context.Context, height int64, limit int64, offset int64) ([]*model.Tx, int64, error) {
	transactions, all, err := s.txRepo.Transactions(ctx, limit, offset, &repository.TxsFilter{TxBlockHeight: &height})
	log.Debug().Msgf("transactions len %d", len(transactions))
	if err != nil {
		return nil, 0, err
	}

	return transactions, all, nil
}

func (s *txs) TransactionSigners(ctx context.Context, hash string) ([]*model.SignerInfo, error) {
	return s.txRepo.TransactionSigners(ctx, hash)
}

func (s *txs) Messages(ctx context.Context, hash string) ([]*model.Message, error) {
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

func (s *txs) GetVotes(ctx context.Context, accountAddress string, uniqueProposals bool, limit int64, offset int64) ([]*model.VotesTransaction, int64, error) {
	return s.txRepo.GetVotes(ctx, accountAddress, uniqueProposals, limit, offset)
}

func (s *txs) GetPowerEvents(ctx context.Context, accountAddress string, limit int64, offset int64) ([]*model.Tx, int64, error) {
	return s.txRepo.GetPowerEvents(ctx, accountAddress, limit, offset)
}

func (s *txs) GetValidatorHistoryEvents(ctx context.Context, accountAddress string,
	limit int64, offset int64,
) ([]*model.Tx, int64, error) {
	return s.txRepo.GetValidatorHistory(ctx, accountAddress, limit, offset)
}

func (s *txs) TransactionsByEventValue(ctx context.Context, values []string,
	messageType []string, limit int64, offset int64,
) ([]*model.Tx, int64, error) {
	return s.txRepo.TransactionsByEventValue(ctx, values, messageType, true, limit, offset)
}

func (s *txs) GetVotesByAccounts(ctx context.Context, accounts []string, excludeAcc bool, voteType string,
	proposalID int, byAccAddress *string, limit int64, offset int64, sortBy *model.SortBy,
) ([]*model.VotesTransaction, int64, error) {
	return s.txRepo.GetVotesByAccounts(ctx, accounts, excludeAcc, voteType, proposalID, byAccAddress, limit, offset, sortBy)
}

func (s *txs) GetWalletsCountPerPeriod(ctx context.Context, startDate, endDate time.Time) (int64, error) {
	return s.txRepo.GetWalletsCountPerPeriod(ctx, startDate, endDate)
}

func (s *txs) GetWalletsWithTx(ctx context.Context, limit int64, offset int64) ([]*model.WalletWithTxs, int64, error) {
	return s.txRepo.GetWalletsWithTx(ctx, limit, offset)
}

func (s *txs) TxCountByAccounts(ctx context.Context, accounts []string) ([]*model.WalletWithTxs, error) {
	return s.txRepo.TxCountByAccounts(ctx, accounts)
}

func (s *txs) AccountInfo(ctx context.Context, account string) (*model.AccountInfo, error) {
	info, err := s.txRepo.AccountInfo(ctx, account)
	if errors.Is(err, pgx.ErrNoRows) {
		return &model.AccountInfo{
			TotalTransactions:    0,
			FirstTransactionDate: time.Now(),
			TotalSpent:           model.DecCoin{Amount: decimal.Zero, Denom: ""},
			TotalReceived:        model.DecCoin{Amount: decimal.Zero, Denom: ""},
		}, nil
	}
	return info, err
}

func (s *txs) DelegatesByValidator(ctx context.Context, from, to time.Time, valoperAddress string,
	limit int64, offset int64,
) (data []*model.Tx, totalSum *model.Denom, all int64, err error) {
	return s.txRepo.DelegatesByValidator(ctx, from, to, valoperAddress, limit, offset)
}

func (s *txs) ProposalDepositors(ctx context.Context, proposalID int,
	sortBy *model.SortBy, limit int64, offset int64,
) ([]*model.ProposalDeposit, int64, error) {
	return s.txRepo.ProposalDepositors(ctx, proposalID, sortBy, limit, offset)
}

func (s *txs) TotalRewardByAccount(ctx context.Context, account string) ([]*model.DecCoin, error) {
	return s.txRepo.TotalRewardByAccount(ctx, account)
}
