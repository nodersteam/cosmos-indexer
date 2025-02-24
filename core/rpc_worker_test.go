package core

import (
	"net/http"
	"testing"

	"github.com/noders-team/cosmos-indexer/clients"
	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/probe"
	"github.com/noders-team/cosmos-indexer/rpc"
	"github.com/stretchr/testify/require"
)

func TestDecoding(t *testing.T) {
	probeCfg := config.Probe{
		AccountPrefix: "celestia",
		ChainName:     "celestia",
		ChainID:       "celestia",
		RPC:           "http://65.21.83.57:26657",
	}
	cl := probe.GetProbeClient(probeCfg)
	rpcClient := rpc.URIClient{
		Address: cl.Config.RPCAddr,
		Client:  &http.Client{},
	}
	chainRpcClient := clients.NewChainRPC(cl)

	blockWorker := NewBlockRPCWorker("id", nil, cl, nil, chainRpcClient)

	blData := blockWorker.FetchBlock(rpcClient, &EnqueueData{
		Height:            2480035,
		IndexTransactions: true,
	})
	require.NotNil(t, blData)

	probeCl := probe.GetProbeClient(probeCfg)
	res, err := blData.MarshalJSON(&probeCl.Codec)
	require.NoError(t, err)

	var data IndexerBlockEventData
	err = data.UnmarshalJSON(res)
	require.NoError(t, err)
	require.NotNil(t, data)
}
