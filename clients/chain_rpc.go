package clients

import (
	probeClient "github.com/DefiantLabs/probe/client"
	probeQuery "github.com/DefiantLabs/probe/query"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	txTypes "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/nodersteam/cosmos-indexer/config"
	"github.com/nodersteam/cosmos-indexer/rpc"
	"time"
)

type ChainRPC interface {
	GetBlock(height int64) (*coretypes.ResultBlock, error)
	GetTxsByBlockHeight(height int64) (*txTypes.GetTxsEventResponse, error)
	IsCatchingUp() (bool, error)
	GetLatestBlockHeight() (int64, error)
	GetLatestBlockHeightWithRetry(retryMaxAttempts int64, retryMaxWaitSeconds uint64) (int64, error)
	GetEarliestAndLatestBlockHeights() (int64, int64, error)
}

type chainRPC struct {
	cl *probeClient.ChainClient
}

func NewChainRPC(cl *probeClient.ChainClient) ChainRPC {
	return &chainRPC{cl: cl}
}

func (c *chainRPC) GetBlock(height int64) (*coretypes.ResultBlock, error) {
	options := probeQuery.QueryOptions{Height: height}
	query := probeQuery.Query{Client: c.cl, Options: &options}
	resp, err := query.Block()
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *chainRPC) GetTxsByBlockHeight(height int64) (*txTypes.GetTxsEventResponse, error) {
	pg := query.PageRequest{Limit: 100, CountTotal: true, Offset: 0}
	options := probeQuery.QueryOptions{Height: height, Pagination: &pg}
	query := probeQuery.Query{Client: c.cl, Options: &options}
	resp, err := query.TxByHeight(c.cl.Codec)
	if err != nil {
		return nil, err
	}

	// handle pagination if needed
	if resp != nil {
		// if there are more total objects than we have so far, keep going
		for resp.Total > uint64(len(resp.Txs)) {
			query.Options.Pagination.Offset = uint64(len(resp.Txs))
			chunkResp, err := query.TxByHeight(c.cl.Codec)
			if err != nil {
				config.Log.Errorf("error getting tx by height %d %s", height, err.Error())
				continue
			}
			resp.Txs = append(resp.Txs, chunkResp.Txs...)
			resp.TxResponses = append(resp.TxResponses, chunkResp.TxResponses...)
		}
	}

	config.Log.Infof("Total fetched transactions for block %d is %d %d", height, len(resp.Txs), len(resp.TxResponses))
	return resp, nil
}

func (c *chainRPC) IsCatchingUp() (bool, error) {
	query := probeQuery.Query{Client: c.cl, Options: &probeQuery.QueryOptions{}}
	ctx, cancel := query.GetQueryContext()
	defer cancel()

	resStatus, err := query.Client.RPCClient.Status(ctx)
	if err != nil {
		return false, err
	}
	return resStatus.SyncInfo.CatchingUp, nil
}

func (c *chainRPC) GetLatestBlockHeight() (int64, error) {
	query := probeQuery.Query{Client: c.cl, Options: &probeQuery.QueryOptions{}}
	ctx, cancel := query.GetQueryContext()
	defer cancel()

	resStatus, err := query.Client.RPCClient.Status(ctx)
	if err != nil {
		return 0, err
	}
	return resStatus.SyncInfo.LatestBlockHeight, nil
}

func (c *chainRPC) GetLatestBlockHeightWithRetry(retryMaxAttempts int64, retryMaxWaitSeconds uint64) (int64, error) {
	if retryMaxAttempts == 0 {
		return c.GetLatestBlockHeight()
	}

	if retryMaxWaitSeconds < 2 {
		retryMaxWaitSeconds = 2
	}

	var attempts int64
	maxRetryTime := time.Duration(retryMaxWaitSeconds) * time.Second
	if maxRetryTime < 0 {
		config.Log.Warn("Detected maxRetryTime overflow, setting time to sane maximum of 30s")
		maxRetryTime = 30 * time.Second
	}

	currentBackoffDuration, maxReached := rpc.GetBackoffDurationForAttempts(attempts, maxRetryTime)

	for {
		resp, err := c.GetLatestBlockHeight()
		attempts++
		if err != nil && (retryMaxAttempts < 0 || (attempts <= retryMaxAttempts)) {
			config.Log.Error("Error getting RPC response, backing off and trying again", err)
			config.Log.Debugf("Attempt %d with wait time %+v", attempts, currentBackoffDuration)
			time.Sleep(currentBackoffDuration)

			// guard against overflow
			if !maxReached {
				currentBackoffDuration, maxReached = rpc.GetBackoffDurationForAttempts(attempts, maxRetryTime)
			}

		} else {
			if err != nil {
				config.Log.Error("Error getting RPC response, reached max retry attempts")
			}
			return resp, err
		}
	}
}

func (c *chainRPC) GetEarliestAndLatestBlockHeights() (int64, int64, error) {
	query := probeQuery.Query{Client: c.cl, Options: &probeQuery.QueryOptions{}}
	ctx, cancel := query.GetQueryContext()
	defer cancel()

	resStatus, err := query.Client.RPCClient.Status(ctx)
	if err != nil {
		return 0, 0, err
	}
	return resStatus.SyncInfo.EarliestBlockHeight, resStatus.SyncInfo.LatestBlockHeight, nil
}
