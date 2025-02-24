package cmd

import (
	"net/http"
	"testing"

	"github.com/noders-team/cosmos-indexer/core"
	"github.com/noders-team/cosmos-indexer/core/tx"
	"github.com/noders-team/cosmos-indexer/filter"

	"github.com/noders-team/cosmos-indexer/clients"
	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/probe"
	"github.com/noders-team/cosmos-indexer/rpc"
	"github.com/stretchr/testify/require"
)

func TestDecoding(t *testing.T) {
	probeCfg := config.Probe{
		AccountPrefix: "0g",
		ChainName:     "0g",
		ChainID:       "zgtendermint_16600-2",
		RPC:           "https://og-testnet-rpc.itrocket.net:443",
	}
	cl := probe.GetProbeClient(probeCfg)
	rpcClient := rpc.URIClient{
		Address: cl.Config.RPCAddr,
		Client:  &http.Client{},
	}
	chainRpcClient := clients.NewChainRPC(cl)

	blockWorker := core.NewBlockRPCWorker("id",
		nil, cl, nil, chainRpcClient)

	blData := blockWorker.FetchBlock(rpcClient, &core.EnqueueData{
		Height:            2494206,
		IndexTransactions: true,
	})
	require.NotNil(t, blData)
	require.Len(t, blData.GetTxsResponse.Txs, 1)

	probeCl := probe.GetProbeClient(probeCfg)
	res, err := blData.MarshalJSON(&probeCl.Codec)
	require.NoError(t, err)

	var data core.IndexerBlockEventData
	err = data.UnmarshalJSON(res)
	require.NoError(t, err)
	require.NotNil(t, data)

	txParser := tx.NewParser(nil, probeCl, tx.NewProcessor(probeCl))
	txDBWrappers, _, err := txParser.ProcessRPCTXs(make([]filter.MessageTypeFilter, 0), blData.GetTxsResponse)
	require.Len(t, txDBWrappers, 1)
}

func TestDecodingTxs(t *testing.T) {
	probeCfg := config.Probe{
		AccountPrefix: "nil",
		ChainName:     "nil",
		ChainID:       "nillion-chain-testnet-1",
		RPC:           "https://nillion-testnet.rpc.kjnodes.com:443",
	}
	cl := probe.GetProbeClient(probeCfg)
	rpcClient := rpc.URIClient{
		Address: cl.Config.RPCAddr,
		Client:  &http.Client{},
	}
	chainRpcClient := clients.NewChainRPC(cl)

	chainCatchingUp, err := chainRpcClient.IsCatchingUp()
	require.NoError(t, err)
	require.False(t, chainCatchingUp)

	blockWorker := core.NewBlockRPCWorker(probeCfg.ChainID,
		nil, cl, nil, chainRpcClient)

	blData := blockWorker.FetchBlock(rpcClient, &core.EnqueueData{
		Height:            9568000,
		IndexTransactions: true,
	})
	require.NotNil(t, blData)
	require.Len(t, blData.BlockResultsData.TxsResults, 3)

	probeCl := probe.GetProbeClient(probeCfg)
	res, err := blData.MarshalJSON(&probeCl.Codec)
	require.NoError(t, err)

	var data core.IndexerBlockEventData
	err = data.UnmarshalJSON(res)
	require.NoError(t, err)
	require.NotNil(t, data)

	txParser := tx.NewParser(nil, probeCl, tx.NewProcessor(probeCl))
	txDBWrappers, _, err := txParser.ProcessRPCBlockByHeightTXs(make([]filter.MessageTypeFilter, 0), blData.BlockData, blData.BlockResultsData)
	require.Len(t, txDBWrappers, 3)
}
