package clients

import (
	"github.com/nodersteam/cosmos-indexer/config"
	"github.com/nodersteam/cosmos-indexer/probe"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_GetTxsByBlockHeight(t *testing.T) {
	cl := probe.GetProbeClient(config.Probe{
		AccountPrefix: "celestia",
		ChainName:     "celestia",
		ChainID:       "celestia",
		RPC:           "http://65.109.54.91:11657",
	})
	rpc := NewChainRPC(cl)
	resp, err := rpc.GetTxsByBlockHeight(534759)
	require.NoError(t, err)
	require.Len(t, resp.Txs, 723)
	require.Len(t, resp.TxResponses, 723)
}
