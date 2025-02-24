package core

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	cmjson "github.com/cometbft/cometbft/libs/json"

	"github.com/rs/zerolog/log"

	"github.com/noders-team/cosmos-indexer/clients"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	txTypes "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/noders-team/cosmos-indexer/config"
	dbTypes "github.com/noders-team/cosmos-indexer/db"
	"github.com/noders-team/cosmos-indexer/rpc"
	"github.com/nodersteam/probe/client"
	probeClient "github.com/nodersteam/probe/client"
	"gorm.io/gorm"
)

type IndexerBlockEventData struct {
	BlockData                *ctypes.ResultBlock
	BlockResultsData         *ctypes.ResultBlockResults
	BlockEventRequestsFailed bool
	GetTxsResponse           *txTypes.GetTxsEventResponse
	TxRequestsFailed         bool
	IndexTransactions        bool
}

func (s *IndexerBlockEventData) MarshalJSON(cdc *probeClient.Codec) ([]byte, error) {
	txResp, err := cdc.Marshaler.Marshal(s.GetTxsResponse)
	if err != nil {
		return nil, err
	}

	blData, err := cmjson.Marshal(s.BlockData)
	if err != nil {
		return nil, err
	}

	blResultsData, err := cmjson.Marshal(s.BlockResultsData)
	if err != nil {
		return nil, err
	}

	data := map[string]interface{}{
		"BlockData":                base64.StdEncoding.EncodeToString(blData),
		"BlockResultsData":         base64.StdEncoding.EncodeToString(blResultsData),
		"BlockEventRequestsFailed": s.BlockEventRequestsFailed,
		"GetTxsResponse":           base64.StdEncoding.EncodeToString(txResp),
		"TxRequestsFailed":         s.TxRequestsFailed,
		"IndexTransactions":        s.IndexTransactions,
	}

	return json.Marshal(&data)
}

func (s *IndexerBlockEventData) UnmarshalJSON(data []byte) error {
	var mapped map[string]interface{}
	err := json.Unmarshal(data, &mapped)
	if err != nil {
		config.Log.Error("json.Unmarshal(data, &mapped)", err)
		return err
	}

	txResp, found := mapped["GetTxsResponse"]
	if found {
		b, err := base64.StdEncoding.DecodeString(txResp.(string))
		if err != nil {
			return err
		}
		var rxResp txTypes.GetTxsEventResponse
		err = rxResp.Unmarshal(b)
		if err != nil {
			config.Log.Error("json.Unmarshal(GetTxsResponse)", err)
			return err
		}
		s.GetTxsResponse = &rxResp
	}

	var blockData ctypes.ResultBlock
	blDataResp, found := mapped["BlockData"]
	if found {
		b, err := base64.StdEncoding.DecodeString(blDataResp.(string))
		if err != nil {
			return err
		}
		err = cmjson.Unmarshal(b, &blockData)
		if err != nil {
			config.Log.Error("json.Unmarshal(BlockData)", err)
			return err
		}
		s.BlockData = &blockData
	}

	var blockResultsData ctypes.ResultBlockResults
	blResult, found := mapped["BlockResultsData"]
	if found {
		b, err := base64.StdEncoding.DecodeString(blResult.(string))
		if err != nil {
			return err
		}
		err = cmjson.Unmarshal(b, &blockResultsData)
		if err != nil {
			config.Log.Error("json.Unmarshal(BlockResultsData)", err)
			return err
		}
		s.BlockResultsData = &blockResultsData
	}

	blockEventRequestsFailed, _ := mapped["BlockEventRequestsFailed"]
	s.BlockEventRequestsFailed = blockEventRequestsFailed.(bool)

	txRequestsFailed, _ := mapped["TxRequestsFailed"]
	s.TxRequestsFailed = txRequestsFailed.(bool)

	indexTransactions, _ := mapped["IndexTransactions"]
	s.IndexTransactions = indexTransactions.(bool)

	return nil
}

type BlockRPCWorker interface {
	Worker(wg *sync.WaitGroup,
		blockEnqueueChan chan *EnqueueData,
		result chan<- IndexerBlockEventData)
	FetchBlock(rpcClient rpc.URIClient,
		block *EnqueueData) *IndexerBlockEventData
}

type blockRPCWorker struct {
	chainStringID string
	cfg           *config.IndexConfig
	chainClient   *client.ChainClient
	db            *gorm.DB
	rpcClient     clients.ChainRPC
}

func NewBlockRPCWorker(
	chainStringID string,
	cfg *config.IndexConfig,
	chainClient *client.ChainClient,
	db *gorm.DB,
	rpcClient clients.ChainRPC,
) BlockRPCWorker {
	return &blockRPCWorker{
		chainStringID: chainStringID,
		cfg:           cfg,
		chainClient:   chainClient,
		db:            db,
		rpcClient:     rpcClient,
	}
}

// Worker This function is responsible for making all RPC requests to the chain needed for later processing.
// The indexer relies on a number of RPC endpoints for full block data, including block event and transaction searches.
func (w *blockRPCWorker) Worker(wg *sync.WaitGroup,
	blockEnqueueChan chan *EnqueueData,
	result chan<- IndexerBlockEventData,
) {
	defer wg.Done()
	rpcClient := rpc.URIClient{
		Address: w.chainClient.Config.RPCAddr,
		Client:  &http.Client{},
	}

	for {
		// Get the next block to process
		block, open := <-blockEnqueueChan
		if !open {
			config.Log.Debugf("Block enqueue channel closed. Exiting RPC worker.")
			break
		}

		tmStart := time.Now()
		log.Debug().Msgf("====> picked ip block for fetching %d %s", block.Height, tmStart.String())
		currentHeightIndexerData := w.FetchBlock(rpcClient, block)
		if currentHeightIndexerData == nil {
			continue
		}

		result <- *currentHeightIndexerData
	}
}

func (w *blockRPCWorker) FetchBlock(rpcClient rpc.URIClient, block *EnqueueData) *IndexerBlockEventData {
	currentHeightIndexerData := IndexerBlockEventData{
		BlockEventRequestsFailed: false,
		TxRequestsFailed:         false,
		IndexTransactions:        block.IndexTransactions,
	}

	// Get the block from the RPC

	blockData, err := w.rpcClient.GetBlock(block.Height)
	if err != nil {
		// This is the only response we continue on. If we can't get the block, we can't index anything.
		config.Log.Errorf("Error getting block %v from RPC. Err: %v", block, err)
		err := dbTypes.UpsertFailedEventBlock(w.db, block.Height, w.chainStringID, w.cfg.Probe.ChainName)
		if err != nil {
			config.Log.Fatal("Failed to insert failed block event", err)
		}
		err = dbTypes.UpsertFailedBlock(w.db, block.Height, w.chainStringID, w.cfg.Probe.ChainName)
		if err != nil {
			config.Log.Fatal("Failed to insert failed block", err)
		}
		return nil
	}

	currentHeightIndexerData.BlockData = blockData
	retryAttempts := int64(5)
	if w.cfg != nil {
		retryAttempts = w.cfg.Base.RequestRetryAttempts
	}

	retryMaxWait := uint64(20)
	if w.cfg != nil {
		retryMaxWait = w.cfg.Base.RequestRetryMaxWait
	}

	if block.IndexTransactions {
		txsEventResp, err := w.rpcClient.GetTxsByBlockHeight(block.Height)

		if err != nil {
			// Attempt to get block results to attempt an in-app codec decode of transactions.
			if currentHeightIndexerData.BlockResultsData == nil {

				bresults, err := rpc.GetBlockResultWithRetry(rpcClient, block.Height,
					retryAttempts, retryMaxWait)

				if err != nil {
					config.Log.Errorf("Error getting txs for block %v from RPC. Err: %v", block, err)
					err := dbTypes.UpsertFailedBlock(w.db, block.Height, w.chainStringID, w.cfg.Probe.ChainName)
					if err != nil {
						config.Log.Fatal("Failed to insert failed block", err)
					}
					currentHeightIndexerData.GetTxsResponse = nil
					currentHeightIndexerData.BlockResultsData = nil
					// Only set failed when we can't get the block results either.
					currentHeightIndexerData.TxRequestsFailed = true
				} else {
					currentHeightIndexerData.BlockResultsData = bresults
				}

			}
		} else {
			currentHeightIndexerData.GetTxsResponse = txsEventResp
		}
	}

	return &currentHeightIndexerData
}
