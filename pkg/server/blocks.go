// Package server implements a simple web server.
package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/noders-team/cosmos-indexer/pkg/repository"

	"github.com/noders-team/cosmos-indexer/db/models"
	"github.com/shopspring/decimal"

	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/noders-team/cosmos-indexer/pkg/service"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/noders-team/cosmos-indexer/proto"
)

type blocksServer struct {
	pb.UnimplementedBlocksServiceServer
	srv           service.Blocks
	srvTx         service.Txs
	srvS          service.Search
	cache         repository.Cache
	srvAggregates service.Aggregates
}

func NewBlocksServer(srv service.Blocks, srvTx service.Txs, srvS service.Search,
	cache repository.Cache, srvAggregates service.Aggregates,
) *blocksServer { //nolint: revive
	return &blocksServer{srv: srv, srvTx: srvTx, srvS: srvS, cache: cache, srvAggregates: srvAggregates}
}

func (r *blocksServer) BlockInfo(ctx context.Context, in *pb.GetBlockInfoRequest) (*pb.GetBlockInfoResponse, error) {
	res, err := r.srv.BlockInfo(ctx, in.BlockNumber)
	if err != nil {
		return &pb.GetBlockInfoResponse{}, err
	}

	return &pb.GetBlockInfoResponse{
		BlockNumber: in.BlockNumber,
		ChainId:     in.ChainId, Info: r.blockToProto(res),
	}, nil
}

func (r *blocksServer) BlockValidators(ctx context.Context, in *pb.GetBlockValidatorsRequest) (*pb.GetBlockValidatorsResponse, error) {
	res, err := r.srv.BlockValidators(ctx, in.BlockNumber)
	if err != nil {
		return &pb.GetBlockValidatorsResponse{}, err
	}

	return &pb.GetBlockValidatorsResponse{BlockNumber: in.BlockNumber, ChainId: in.ChainId, ValidatorsList: res}, nil
}

func (r *blocksServer) TxChartByDay(ctx context.Context, in *pb.TxChartByDayRequest) (*pb.TxChartByDayResponse, error) {
	res, err := r.srvTx.ChartTxByDay(ctx, in.From.AsTime(), in.To.AsTime())
	if err != nil {
		return &pb.TxChartByDayResponse{}, err
	}
	data := make([]*pb.TxByDay, 0)
	for _, tx := range res {
		data = append(data, &pb.TxByDay{
			TxNum: tx.TxNum,
			Day:   timestamppb.New(tx.Day),
		})
	}

	return &pb.TxChartByDayResponse{TxByDay: data}, nil
}

func (r *blocksServer) TxByHash(ctx context.Context, in *pb.TxByHashRequest) (*pb.TxByHashResponse, error) {
	res, err := r.srvTx.GetTxByHash(ctx, in.Hash)
	if err != nil {
		return &pb.TxByHashResponse{}, err
	}
	return &pb.TxByHashResponse{
		Tx: r.txToProto(res),
	}, nil
}

func (r *blocksServer) TotalTransactions(ctx context.Context, in *pb.TotalTransactionsRequest) (*pb.TotalTransactionsResponse, error) {
	res, err := r.srvTx.TotalTransactions(ctx, in.To.AsTime())
	if err != nil {
		return &pb.TotalTransactionsResponse{}, err
	}
	return r.toTotalTransactionsProto(res), nil
}

func (r *blocksServer) toTotalTransactionsProto(res *model.TotalTransactions) *pb.TotalTransactionsResponse {
	return &pb.TotalTransactionsResponse{
		Total:     fmt.Sprintf("%d", res.Total),
		Total24H:  fmt.Sprintf("%d", res.Total24H),
		Total30D:  fmt.Sprintf("%d", res.Total30D),
		Total48H:  fmt.Sprintf("%d", res.Total48H),
		Volume24H: res.Volume24H.String(),
		Volume30D: res.Volume30D.String(),
	}
}

func (r *blocksServer) Transactions(ctx context.Context, in *pb.TransactionsRequest) (*pb.TransactionsResponse, error) {
	transactions, total, err := r.srvTx.Transactions(ctx, in.Limit.Offset, in.Limit.Limit)
	if err != nil {
		return &pb.TransactionsResponse{}, err
	}

	res := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		res = append(res, r.txToProto(transaction))
	}

	return &pb.TransactionsResponse{
		Tx:     res,
		Result: &pb.Result{Limit: in.Limit.Limit, Offset: in.Limit.Offset, All: total},
	}, nil
}

func (r *blocksServer) CacheTransactions(ctx context.Context, in *pb.TransactionsRequest) (*pb.TransactionsResponse, error) {
	transactions, total, err := r.cache.GetTransactions(ctx, in.Limit.Offset, in.Limit.Limit)
	if err != nil {
		return &pb.TransactionsResponse{}, err
	}

	res := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		res = append(res, r.txToProto(transaction))
	}

	return &pb.TransactionsResponse{
		Tx:     res,
		Result: &pb.Result{Limit: in.Limit.Limit, Offset: in.Limit.Offset, All: total},
	}, nil
}

func (r *blocksServer) TotalBlocks(ctx context.Context, in *pb.TotalBlocksRequest) (*pb.TotalBlocksResponse, error) {
	blocks, err := r.srv.TotalBlocks(ctx, in.To.AsTime())
	if err != nil {
		return &pb.TotalBlocksResponse{}, err
	}
	return r.toTotalBlocksProto(blocks), nil
}

func (r *blocksServer) toTotalBlocksProto(blocks *model.TotalBlocks) *pb.TotalBlocksResponse {
	return &pb.TotalBlocksResponse{
		Height:      blocks.BlockHeight,
		Count24H:    blocks.Count24H,
		Count48H:    blocks.Count48H,
		Time:        blocks.BlockTime,
		TotalFee24H: blocks.TotalFee24H.String(),
	}
}

func (r *blocksServer) GetBlocks(ctx context.Context, in *pb.GetBlocksRequest) (*pb.GetBlocksResponse, error) {
	blocks, all, err := r.srv.Blocks(ctx, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.GetBlocksResponse{}, err
	}

	res := make([]*pb.Block, 0)
	for _, bl := range blocks {
		res = append(res, r.blockToProto(bl))
	}
	return &pb.GetBlocksResponse{Blocks: res, Result: &pb.Result{Limit: in.Limit.Limit, Offset: in.Limit.Offset, All: all}}, nil
}

func (r *blocksServer) CacheGetBlocks(ctx context.Context, in *pb.GetBlocksRequest) (*pb.GetBlocksResponse, error) {
	blocks, total, err := r.cache.GetBlocks(ctx, in.Limit.Offset, in.Limit.Limit)
	if err != nil {
		return &pb.GetBlocksResponse{}, err
	}

	res := make([]*pb.Block, 0)
	for _, bl := range blocks {
		res = append(res, r.blockToProto(bl))
	}
	return &pb.GetBlocksResponse{Blocks: res, Result: &pb.Result{Limit: in.Limit.Limit, Offset: in.Limit.Offset, All: total}}, nil
}

func (r *blocksServer) blockToProto(bl *model.BlockInfo) *pb.Block {
	return &pb.Block{
		BlockHeight:       bl.BlockHeight,
		ProposedValidator: bl.ProposedValidatorAddress,
		GenerationTime:    timestamppb.New(bl.GenerationTime),
		TxHash:            bl.BlockHash,
		TotalTx:           bl.TotalTx,
		GasUsed:           bl.GasUsed.String(),
		GasWanted:         bl.GasWanted.String(),
		TotalFees:         bl.TotalFees.String(),
		BlockRewards:      decimal.NewFromInt(0).String(),
	}
}

func (r *blocksServer) BlockSignatures(ctx context.Context, in *pb.BlockSignaturesRequest) (*pb.BlockSignaturesResponse, error) {
	signs, all, err := r.srv.BlockSignatures(ctx, in.BlockHeight, in.ValAddress, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.BlockSignaturesResponse{}, err
	}

	data := make([]*pb.SignerAddress, 0)
	for _, sign := range signs {
		data = append(data, r.blockSignToProto(sign))
	}

	return &pb.BlockSignaturesResponse{
		Signers: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) blockSignToProto(in *model.BlockSigners) *pb.SignerAddress {
	return &pb.SignerAddress{
		Address: in.Validator,
		Time:    timestamppb.New(in.Time),
		Rank:    in.Rank,
	}
}

func (r *blocksServer) TxsByBlock(ctx context.Context, in *pb.TxsByBlockRequest) (*pb.TxsByBlockResponse, error) {
	transactions, all, err := r.srvTx.TransactionsByBlock(ctx, in.BlockHeight, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.TxsByBlockResponse{}, err
	}

	data := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		data = append(data, r.txToProto(transaction))
	}

	return &pb.TxsByBlockResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) TransactionRawLog(ctx context.Context, in *pb.TransactionRawLogRequest) (*pb.TransactionRawLogResponse, error) {
	resp, err := r.srvTx.TransactionRawLog(ctx, in.TxHash)
	if err != nil {
		return &pb.TransactionRawLogResponse{}, err
	}

	return &pb.TransactionRawLogResponse{RawLog: resp}, nil
}

func (r *blocksServer) txToProto(tx *model.Tx) *pb.TxByHash {
	return &pb.TxByHash{
		Memo:                        tx.Memo,
		TimeoutHeight:               fmt.Sprintf("%d", tx.TimeoutHeight),
		ExtensionOptions:            tx.ExtensionOptions,
		NonCriticalExtensionOptions: tx.NonCriticalExtensionOptions,
		AuthInfo: &pb.TxAuthInfo{
			PublicKey:  []string{}, // TODO
			Signatures: tx.Signatures,
			Fee: &pb.TxFee{
				Granter:  tx.AuthInfo.Fee.Granter,
				Payer:    tx.AuthInfo.Fee.Payer,
				GasLimit: fmt.Sprintf("%d", tx.AuthInfo.Fee.GasLimit),
				Amount:   nil, // TODO
			},
			Tip: &pb.TxTip{
				Tipper: tx.AuthInfo.Tip.Tipper,
				Amount: r.txTipToProto(tx.AuthInfo.Tip.Amount),
			},
		},
		Fees: r.toFeesProto(tx.Fees),
		TxResponse: &pb.TxResponse{
			Height:    tx.TxResponse.Height,
			Txhash:    tx.Hash,
			Codespace: tx.TxResponse.Codespace,
			Code:      int32(tx.TxResponse.Code),
			Data:      tx.TxResponse.Data,
			Info:      tx.TxResponse.Info,
			GasWanted: fmt.Sprintf("%d", tx.TxResponse.GasWanted),
			GasUsed:   fmt.Sprintf("%d", tx.TxResponse.GasUsed),
			Timestamp: tx.TxResponse.TimeStamp,
		},
		Block:          r.toBlockProto(&tx.Block),
		SenderReceiver: r.txSenderToProto(tx.SenderReceiver),
		Events:         r.toEventsProto(tx.Events),
	}
}

func (r *blocksServer) toEventsProto(in []*model.TxEvents) []*pb.TxEvent {
	if in == nil {
		return nil
	}

	res := make([]*pb.TxEvent, 0)
	for _, event := range in {
		res = append(res, &pb.TxEvent{
			MessageType: event.MessageType,
			EventIndex:  int32(event.EventIndex),
			Type:        event.Type,
			Index:       int32(event.Index),
			Value:       event.Value,
			Key:         event.Key,
		})
	}

	return res
}

func (r *blocksServer) txSenderToProto(in *model.TxSenderReceiver) *pb.TxSenderReceiver {
	if in == nil {
		return nil
	}
	return &pb.TxSenderReceiver{
		MessageType: in.MessageType,
		Sender:      in.Sender,
		Receiver:    in.Receiver,
		Amount:      in.Amount,
		Denom:       in.Denom,
	}
}

func (r *blocksServer) toFeesProto(fees []model.Fee) []*pb.Fee {
	res := make([]*pb.Fee, 0)
	for _, fee := range fees {
		res = append(res, &pb.Fee{
			Amount: fee.Amount.String(),
			Denom:  fee.Denomination.Base,
			Payer:  fee.PayerAddress.Address,
		})
	}
	return res
}

func (r *blocksServer) toBlockProto(bl *models.Block) *pb.Block {
	return &pb.Block{
		BlockHeight:       bl.Height,
		ProposedValidator: bl.ProposerConsAddress.Address,
		TxHash:            bl.BlockHash,
		GenerationTime:    timestamppb.New(bl.TimeStamp),
	}
}

func (r *blocksServer) txTipToProto(tips []model.TipAmount) []*pb.Denom {
	denoms := make([]*pb.Denom, 0)
	for _, tip := range tips {
		denoms = append(denoms, &pb.Denom{
			Denom:  tip.Denom,
			Amount: tip.Amount.String(),
		})
	}
	return denoms
}

func (r *blocksServer) TransactionSigners(ctx context.Context, in *pb.TransactionSignersRequest) (*pb.TransactionSignersResponse, error) {
	resp, err := r.srvTx.TransactionSigners(ctx, in.TxHash)
	if err != nil {
		return &pb.TransactionSignersResponse{}, err
	}

	return &pb.TransactionSignersResponse{Signers: r.toSignerInfosProto(resp)}, nil
}

func (r *blocksServer) toSignerInfosProto(signs []*model.SignerInfo) []*pb.SignerInfo {
	res := make([]*pb.SignerInfo, 0)
	for _, sign := range signs {
		res = append(res, &pb.SignerInfo{
			Address:  sign.Address.Address,
			ModeInfo: sign.ModeInfo,
			Sequence: int64(sign.Sequence),
		})
	}
	return res
}

func (r *blocksServer) CacheAggregated(ctx context.Context,
	_ *pb.CacheAggregatedRequest,
) (*pb.CacheAggregatedResponse, error) {
	info, err := r.srvAggregates.GetTotals(ctx)
	if err != nil {
		log.Err(err).Msgf("failed to get totals from cache, requesting new %v", err)
		return &pb.CacheAggregatedResponse{}, errors.New("totals not found")
	}

	// TODO not the best place
	lastBlock, err := r.srv.LatestBlockHeight(ctx)
	if err == nil {
		info.Blocks.BlockHeight = lastBlock
	}

	return &pb.CacheAggregatedResponse{
		Transactions: r.toTotalTransactionsProto(&info.Transactions),
		Blocks:       r.toTotalBlocksProto(&info.Blocks),
		Wallets: &pb.TotalWallets{
			Total:     info.Wallets.Total,
			Count_24H: info.Wallets.Count24H,
			Count_48H: info.Wallets.Count48H,
			Count_30D: info.Wallets.Count30D,
		},
	}, nil
}

func (r *blocksServer) SearchHashByText(ctx context.Context, in *pb.SearchHashByTextRequest) (*pb.SearchHashByTextResponse, error) {
	searchStr := in.Text

	log.Info().Msgf("SearchHashByText: %s", searchStr)
	res, err := r.srvS.SearchByText(ctx, searchStr)
	if err != nil {
		return &pb.SearchHashByTextResponse{}, err
	}

	data := make([]*pb.SearchResults, 0)
	for _, s := range res {
		data = append(data, &pb.SearchResults{
			Hash:        s.TxHash,
			HashType:    s.Type,
			BlockHeight: s.BlockHeight,
		})
	}

	// hard-limit for search by block number
	if len(searchStr) > 3 {
		height, err := strconv.Atoi(searchStr)
		if err == nil {
			res, _ = r.srvS.SearchByBlock(ctx, int64(height))
			for _, s := range res {
				data = append(data, &pb.SearchResults{
					Hash:        s.TxHash,
					HashType:    s.Type,
					BlockHeight: s.BlockHeight,
				})
			}
		}
	}

	return &pb.SearchHashByTextResponse{Results: data}, nil
}

func (r *blocksServer) BlockInfoByHash(ctx context.Context, in *pb.BlockInfoByHashRequest) (*pb.BlockInfoByHashResponse, error) {
	res, err := r.srv.BlockInfoByHash(ctx, in.Hash)
	if err != nil {
		return &pb.BlockInfoByHashResponse{}, err
	}

	return &pb.BlockInfoByHashResponse{Info: r.blockToProto(res)}, nil
}

func (r *blocksServer) ChartTransactionsByHour(ctx context.Context, in *pb.ChartTransactionsByHourRequest) (*pb.ChartTransactionsByHourResponse, error) {
	res, err := r.srvTx.ChartTransactionsByHour(ctx, in.To.AsTime())
	if err != nil {
		return &pb.ChartTransactionsByHourResponse{}, err
	}
	return &pb.ChartTransactionsByHourResponse{
		Total_24H: res.Total24H,
		Total_48H: res.Total48H,
		Points:    r.toPointsPb(res.Points),
	}, nil
}

func (r *blocksServer) toPointsPb(in []*model.TxsByHour) []*pb.TxsByHour {
	data := make([]*pb.TxsByHour, 0)
	for _, tx := range in {
		data = append(data, &pb.TxsByHour{
			TxNum: int64(tx.TxNum),
			Hour:  timestamppb.New(tx.Hour),
		})
	}
	return data
}

func (r *blocksServer) ChartTransactionsVolume(ctx context.Context, in *pb.ChartTransactionsVolumeRequest) (*pb.ChartTransactionsVolumeResponse, error) {
	res, err := r.srvTx.ChartTransactionsVolume(ctx, in.To.AsTime())
	if err != nil {
		return &pb.ChartTransactionsVolumeResponse{}, err
	}

	data := make([]*pb.TxsVolumeByHour, 0)
	for _, tx := range res {
		data = append(data, &pb.TxsVolumeByHour{
			TxVolume: tx.TxVolume.String(),
			Hour:     timestamppb.New(tx.Hour),
		})
	}
	return &pb.ChartTransactionsVolumeResponse{Points: data}, nil
}

func (r *blocksServer) BlockUpTime(ctx context.Context, in *pb.BlockUpTimeRequest) (*pb.BlockUpTimeResponse, error) {
	upTime, err := r.srv.BlockUptime(ctx, in.BlockWindow, in.BlockHeight, in.ValidatorAddress)
	if err != nil {
		return &pb.BlockUpTimeResponse{}, err
	}

	return &pb.BlockUpTimeResponse{Uptime: decimal.NewFromFloat32(upTime).String()}, nil
}

func (r *blocksServer) UptimeByBlocks(ctx context.Context, in *pb.UptimeByBlocksRequest) (*pb.UptimeByBlocksResponse, error) {
	unSinged, total, err := r.srv.UptimeByBlocks(ctx, in.BlockWindow, in.BlockHeight, in.ValidatorAddress)
	if err != nil {
		return &pb.UptimeByBlocksResponse{}, err
	}
	data := make([]*pb.BlockSigned, 0)
	for _, s := range unSinged {
		data = append(data, &pb.BlockSigned{BlockHeight: s.BlockHeight, Signed: s.Signed})
	}

	return &pb.UptimeByBlocksResponse{Uptime: total, Blocks: data}, nil
}

func (r *blocksServer) GetVotes(ctx context.Context, in *pb.GetVotesRequest) (*pb.GetVotesResponse, error) {
	txs, all, err := r.srvTx.GetVotes(ctx, in.ValidatorAccountAddress, in.UniqueProposals, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.GetVotesResponse{}, err
	}

	res := make([]*pb.VotesTransaction, 0)
	for _, tx := range txs {
		res = append(res, &pb.VotesTransaction{
			BlockHeight: tx.BlockHeight,
			TxHash:      tx.TxHash,
			ProposalId:  int32(tx.ProposalID),
			Voter:       tx.Voter,
			Option:      tx.Option,
			Weight:      tx.Weight,
			Time:        timestamppb.New(tx.Timestamp),
			Tx:          r.txToProto(tx.Tx),
		})
	}

	return &pb.GetVotesResponse{
		Transactions: res,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) GetPowerEvents(ctx context.Context, in *pb.GetPowerEventsRequest) (*pb.GetPowerEventsResponse, error) {
	transactions, all, err := r.srvTx.GetPowerEvents(ctx, in.ValidatorAccountAddress, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.GetPowerEventsResponse{}, err
	}

	data := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		data = append(data, r.txToProto(transaction))
	}

	return &pb.GetPowerEventsResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) GetValidatorHistoryEvents(ctx context.Context, in *pb.GetValidatorHistoryEventsRequest) (*pb.GetValidatorHistoryEventsResponse, error) {
	transactions, all, err := r.srvTx.GetValidatorHistoryEvents(ctx, in.ValidatorAccountAddress, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.GetValidatorHistoryEventsResponse{}, err
	}

	data := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		data = append(data, r.txToProto(transaction))
	}

	return &pb.GetValidatorHistoryEventsResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) TransactionsByEventValue(ctx context.Context,
	in *pb.TransactionsByEventValueRequest,
) (*pb.TransactionsByEventValueResponse, error) {
	transactions, all, err := r.srvTx.TransactionsByEventValue(ctx, in.Values, in.Type, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.TransactionsByEventValueResponse{}, err
	}

	data := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		data = append(data, r.txToProto(transaction))
	}

	return &pb.TransactionsByEventValueResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) GetVotesByAccounts(ctx context.Context,
	in *pb.GetVotesByAccountsRequest,
) (*pb.GetVotesByAccountsResponse, error) {
	var sortBy *model.SortBy
	if in.Sort != nil {
		sortBy = &model.SortBy{
			By:        in.Sort.SortBy,
			Direction: in.Sort.Direction,
		}
	}

	transactions, all, err := r.srvTx.GetVotesByAccounts(ctx, in.Accounts, in.Exclude,
		in.VoteType, int(in.ProposalID), in.AccountAddr, in.Limit.Limit, in.Limit.Offset, sortBy)
	if err != nil {
		return &pb.GetVotesByAccountsResponse{}, err
	}

	data := make([]*pb.VotesTransaction, 0)
	for _, tx := range transactions {
		data = append(data, &pb.VotesTransaction{
			BlockHeight: tx.BlockHeight,
			TxHash:      tx.TxHash,
			ProposalId:  int32(tx.ProposalID),
			Voter:       tx.Voter,
			Option:      tx.Option,
			Weight:      tx.Weight,
			Time:        timestamppb.New(tx.Timestamp),
		})
	}

	return &pb.GetVotesByAccountsResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) GetWalletsCountPerPeriod(ctx context.Context,
	in *pb.GetWalletsCountPerPeriodRequest,
) (*pb.GetWalletsCountPerPeriodResponse, error) {
	count, err := r.srvTx.GetWalletsCountPerPeriod(ctx, in.Start.AsTime(), in.End.AsTime())
	if err != nil {
		return &pb.GetWalletsCountPerPeriodResponse{}, err
	}
	return &pb.GetWalletsCountPerPeriodResponse{Result: count}, nil
}

func (r *blocksServer) GetWalletsWithTx(ctx context.Context, in *pb.GetWalletsWithTxRequest) (*pb.GetWalletsWithTxResponse, error) {
	res, total, err := r.srvTx.GetWalletsWithTx(ctx, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.GetWalletsWithTxResponse{}, err
	}

	data := make([]*pb.WalletWithTxs, 0)
	for _, tx := range res {
		data = append(data, &pb.WalletWithTxs{
			Account: tx.Account,
			TxCount: tx.TxCount,
		})
	}

	return &pb.GetWalletsWithTxResponse{Data: data, Result: &pb.Result{
		Limit:  in.Limit.Limit,
		Offset: in.Limit.Offset,
		All:    total,
	}}, nil
}

func (r *blocksServer) TxCountByAccounts(ctx context.Context, in *pb.TxCountByAccountsRequest) (*pb.TxCountByAccountsResponse, error) {
	res, err := r.srvTx.TxCountByAccounts(ctx, in.Accounts)
	if err != nil {
		return &pb.TxCountByAccountsResponse{}, err
	}

	data := make([]*pb.WalletWithTxs, 0)
	for _, tx := range res {
		data = append(data, &pb.WalletWithTxs{
			Account: tx.Account,
			TxCount: tx.TxCount,
		})
	}

	return &pb.TxCountByAccountsResponse{Data: data}, nil
}

func (r *blocksServer) AccountInfo(ctx context.Context, in *pb.AccountInfoRequest) (*pb.AccountInfoResponse, error) {
	res, err := r.srvTx.AccountInfo(ctx, in.Account)
	if err != nil {
		return &pb.AccountInfoResponse{}, err
	}

	return &pb.AccountInfoResponse{
		TxCount:     res.TotalTransactions,
		FirstTxTime: timestamppb.New(res.FirstTransactionDate),
		TotalSpent: &pb.Denom{
			Denom:  res.TotalSpent.Denom,
			Amount: res.TotalSpent.Amount.String(),
		},
		TotalReceived: &pb.Denom{
			Denom:  res.TotalReceived.Denom,
			Amount: res.TotalReceived.Amount.String(),
		},
	}, nil
}

func (r *blocksServer) DelegatesByValidator(ctx context.Context, in *pb.DelegatesByValidatorRequest) (*pb.DelegatesByValidatorResponse, error) {
	transactions, sum, total, err := r.srvTx.DelegatesByValidator(ctx,
		in.From.AsTime(), in.To.AsTime(), in.ValoperAddress, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.DelegatesByValidatorResponse{}, err
	}

	data := make([]*pb.TxByHash, 0)
	for _, tx := range transactions {
		transaction := tx
		data = append(data, r.txToProto(transaction))
	}

	return &pb.DelegatesByValidatorResponse{
		TotalSum: &pb.Denom{Amount: sum.Amount, Denom: sum.Denom},
		Tx:       data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    total,
		},
	}, nil
}

func (r *blocksServer) ProposalDepositors(ctx context.Context,
	in *pb.ProposalDepositorsRequest,
) (*pb.ProposalDepositorsResponse, error) {
	var sortBy *model.SortBy
	if in.Sort != nil {
		sortBy = &model.SortBy{
			By:        in.Sort.SortBy,
			Direction: in.Sort.Direction,
		}
	}
	res, all, err := r.srvTx.ProposalDepositors(ctx, int(in.ProposalId), sortBy, in.Limit.Limit, in.Limit.Offset)
	if err != nil {
		return &pb.ProposalDepositorsResponse{}, err
	}

	data := make([]*pb.ProposalDeposit, 0)
	for _, tx := range res {
		data = append(data, &pb.ProposalDeposit{
			TxHash:  tx.TxHash,
			Time:    timestamppb.New(tx.TxTime),
			Address: tx.Address,
			Amount: &pb.Denom{
				Denom:  tx.Amount.Denom,
				Amount: tx.Amount.Amount.String(),
			},
		})
	}

	return &pb.ProposalDepositorsResponse{
		Data: data,
		Result: &pb.Result{
			Limit:  in.Limit.Limit,
			Offset: in.Limit.Offset,
			All:    all,
		},
	}, nil
}

func (r *blocksServer) RewardByAccount(ctx context.Context, in *pb.RewardByAccountRequest) (*pb.RewardByAccountResponse, error) {
	res, err := r.srvTx.TotalRewardByAccount(ctx, in.Account)
	if err != nil {
		return nil, err
	}

	data := make([]*pb.Denom, 0)
	for _, reward := range res {
		data = append(data, &pb.Denom{
			Denom:  reward.Denom,
			Amount: reward.Amount.String(),
		})
	}

	return &pb.RewardByAccountResponse{Amount: data}, nil
}
