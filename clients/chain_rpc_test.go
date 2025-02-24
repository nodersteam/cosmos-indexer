package clients

import (
	"testing"

	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/probe"
	"github.com/stretchr/testify/require"
)

func Test_GetTxsByBlockHeight(t *testing.T) {
	cl := probe.GetProbeClient(config.Probe{
		AccountPrefix: "celestia",
		ChainName:     "celestia",
		ChainID:       "celestia",
		RPC:           "http://celestia-mainnet-consensus.itrocket.net:26657",
	})
	rpc := NewChainRPC(cl)
	resp, err := rpc.GetTxsByBlockHeight(2480035)
	require.NoError(t, err)
	require.Len(t, resp.Txs, 723)
	require.Len(t, resp.TxResponses, 723)
}
