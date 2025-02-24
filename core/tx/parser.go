package tx

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/noders-team/cosmos-indexer/pkg/model"

	"github.com/rs/zerolog/log"

	"github.com/noders-team/cosmos-indexer/core"
	"github.com/shopspring/decimal"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cosmos/cosmos-sdk/types"
	cosmosTx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/noders-team/cosmos-indexer/config"
	txtypes "github.com/noders-team/cosmos-indexer/cosmos/modules/tx"
	dbTypes "github.com/noders-team/cosmos-indexer/db"
	"github.com/noders-team/cosmos-indexer/filter"
	"github.com/nodersteam/probe/client"
	"gorm.io/gorm"
)

type Parser interface {
	ProcessRPCBlockByHeightTXs(messageTypeFilters []filter.MessageTypeFilter,
		blockResults *coretypes.ResultBlock, resultBlockRes *coretypes.ResultBlockResults) ([]dbTypes.TxDBWrapper, *time.Time, error)
	ProcessRPCTXs(messageTypeFilters []filter.MessageTypeFilter,
		txEventResp *cosmosTx.GetTxsEventResponse) ([]dbTypes.TxDBWrapper, *time.Time, error)
}

type parser struct {
	db        *gorm.DB
	cl        *client.ChainClient
	processor Processor
}

func NewParser(db *gorm.DB, cl *client.ChainClient, processor Processor) Parser {
	return &parser{db: db, cl: cl, processor: processor}
}

func (a *parser) ProcessRPCBlockByHeightTXs(messageTypeFilters []filter.MessageTypeFilter,
	blockResults *coretypes.ResultBlock, resultBlockRes *coretypes.ResultBlockResults,
) ([]dbTypes.TxDBWrapper, *time.Time, error) {
	if len(blockResults.Block.Txs) != len(resultBlockRes.TxsResults) {
		config.Log.Fatalf("blockResults & resultBlockRes: different length")
	}

	blockTime := &blockResults.Block.Time
	blockTimeStr := blockTime.Format(time.RFC3339)
	currTxDbWrappers := make([]dbTypes.TxDBWrapper, len(blockResults.Block.Txs))

	for txIdx, tendermintTx := range blockResults.Block.Txs {
		txResult := resultBlockRes.TxsResults[txIdx]

		// Indexer types only used by the indexer app (similar to the cosmos types)
		var indexerMergedTx txtypes.MergedTx
		var indexerTx txtypes.IndexerTx
		var txBody txtypes.Body
		var currMessages []types.Msg
		var currLogMsgs []txtypes.LogMessage

		txDecoder := a.cl.Codec.TxConfig.TxDecoder()

		txBasic, err := txDecoder(tendermintTx)
		var txFull *cosmosTx.Tx
		if err != nil {
			txBasic, err = core.InAppTxDecoder(a.cl.Codec)(tendermintTx)
			if err != nil {
				return nil, blockTime, fmt.Errorf("ProcessRPCBlockByHeightTXs: TX cannot be parsed from block %v. This is usually a proto definition error. Err: %v", blockResults.Block.Height, err)
			}
			txFull = txBasic.(*cosmosTx.Tx)
		} else {
			// This is a hack, but as far as I can tell necessary. "wrapper" struct is private in Cosmos SDK.
			field := reflect.ValueOf(txBasic).Elem().FieldByName("tx")
			iTx := a.getUnexportedField(field)
			txFull = iTx.(*cosmosTx.Tx)
		}

		logs := types.ABCIMessageLogs{}
		// Failed TXs do not have proper JSON in the .Log field, causing ParseABCILogs to fail to unmarshal the logs
		// We can entirely ignore failed TXs in downstream parsers, because according to the Cosmos specification, a single failed message in a TX fails the whole TX
		if txResult.Code == 0 && strings.Trim(txResult.Log, "") != "" {
			logs, err = types.ParseABCILogs(txResult.Log)
			if err != nil {
				log.Warn().Msgf("logs cannot be parsed  using events instead %+v, %s", txResult, err.Error())
			}
		}

		txHash := tendermintTx.Hash()

		var messagesRaw [][]byte

		events := make(types.StringEvents, 0)
		if len(logs) == 0 {
			for _, event := range txResult.Events {
				attr := make([]types.Attribute, 0)
				for _, attribute := range event.Attributes {
					attr = append(attr, types.Attribute{
						Key:   attribute.Key,
						Value: attribute.Value,
					})
				}
				events = append(events, types.StringEvent{
					Type:       event.Type,
					Attributes: attr,
				})
			}
		}

		// Get the Messages and Message Logs
		for msgIdx := range txFull.Body.Messages {
			shouldIndex, err := a.messageTypeShouldIndex(txFull.Body.Messages[msgIdx].TypeUrl, messageTypeFilters)
			if err != nil {
				config.Log.Error("messageTypeShouldIndex", err)
				return nil, blockTime, err
			}

			if !shouldIndex {
				config.Log.Debug(fmt.Sprintf("[Block: %v] [TX: %v] Skipping msg of type '%v'.",
					blockResults.Block.Height, tendermintHashToHex(txHash), txFull.Body.Messages[msgIdx].TypeUrl))
				currMessages = append(currMessages, nil)
				currLogMsgs = append(currLogMsgs, txtypes.LogMessage{
					MessageIndex: msgIdx,
				})
				messagesRaw = append(messagesRaw, nil)
				continue
			}

			currMsg := txFull.Body.Messages[msgIdx].GetCachedValue()

			if currMsg != nil {
				msg := currMsg.(types.Msg)
				messagesRaw = append(messagesRaw, txFull.Body.Messages[msgIdx].Value)
				currMessages = append(currMessages, msg)
				msgEvents := types.StringEvents{}
				if txResult.Code == 0 {
					if len(logs) > 0 {
						msgEvents = logs[msgIdx].Events
					} else {
						msgEvents = events
					}
				}

				currTxLog := txtypes.LogMessage{
					MessageIndex: msgIdx,
					Events:       a.toEvents(msgEvents),
				}
				currLogMsgs = append(currLogMsgs, currTxLog)
			}
		}

		txBody.Messages = currMessages
		indexerTx.Body = txBody

		indexerTxResp := txtypes.Response{
			TxHash:    tendermintHashToHex(txHash),
			Height:    fmt.Sprintf("%d", blockResults.Block.Height),
			TimeStamp: blockTimeStr,
			RawLog:    []byte(txResult.Log),
			Log:       currLogMsgs,
			Code:      txResult.Code,
			GasUsed:   txResult.GasUsed,
			GasWanted: txResult.GasWanted,
			Codespace: txResult.Codespace,
			Info:      txResult.Info,
			// Data:      string(txResult.Data), TODO
		}

		indexerTx.AuthInfo = *txFull.AuthInfo
		indexerMergedTx.TxResponse = indexerTxResp
		indexerMergedTx.Tx = indexerTx
		indexerMergedTx.Tx.AuthInfo = *txFull.AuthInfo

		processedTx, _, err := a.processor.ProcessTx(indexerMergedTx, messagesRaw)
		if err != nil {
			config.Log.Error("ProcessTx", err)
			return currTxDbWrappers, blockTime, err
		}

		filteredSigners := []types.AccAddress{}
		for _, filteredMessage := range txBody.Messages {
			if filteredMessage != nil {
				err := filteredMessage.ValidateBasic()
				if err != nil {
					config.Log.Error("error validating filtered message, ignoring", err)
					continue
				}
				filteredSigners = append(filteredSigners, filteredMessage.GetSigners()...)
			}
		}

		signers, signerInfos, err := a.processor.ProcessSigners(txFull.AuthInfo, filteredSigners)
		if err != nil {
			config.Log.Error("ProcessSigners", err)
			return currTxDbWrappers, blockTime, err
		}

		processedTx.Tx.SignerAddresses = signers

		fees, err := a.processor.ProcessFees(indexerTx.AuthInfo, signers)
		if err != nil {
			config.Log.Error("ProcessFees", err)
			return currTxDbWrappers, blockTime, err
		}

		processedTx.Tx.Fees = fees

		// extra fields
		processedTx.Tx.Signatures = txFull.Signatures
		processedTx.Tx.Memo = txFull.Body.Memo
		processedTx.Tx.TimeoutHeight = txFull.Body.TimeoutHeight

		extensionOptions := make([]string, 0)
		for _, opt := range txFull.Body.ExtensionOptions {
			extensionOptions = append(extensionOptions, opt.String())
		}
		processedTx.Tx.ExtensionOptions = extensionOptions

		nonExtensionOptions := make([]string, 0)
		for _, opt := range txFull.Body.NonCriticalExtensionOptions {
			extensionOptions = append(extensionOptions, opt.String())
		}
		processedTx.Tx.NonCriticalExtensionOptions = nonExtensionOptions
		processedTx.Tx.TxResponse = model.TxResponse{
			TxHash:    indexerTxResp.TxHash,
			Height:    indexerTxResp.Height,
			TimeStamp: indexerTxResp.TimeStamp,
			Code:      indexerTxResp.Code,
			RawLog:    indexerTxResp.RawLog,
			GasUsed:   indexerTxResp.GasUsed,
			GasWanted: indexerTxResp.GasWanted,
			Codespace: indexerTxResp.Codespace,
			Data:      indexerTxResp.Data,
			Info:      indexerTxResp.Info,
		}

		if txFull.AuthInfo != nil && txFull.AuthInfo.Fee != nil {
			txAuthInfo := model.AuthInfo{
				Fee: model.AuthInfoFee{
					Granter:  txFull.AuthInfo.Fee.Granter,
					Payer:    txFull.AuthInfo.Fee.Payer,
					GasLimit: txFull.AuthInfo.Fee.GasLimit,
				},
				SignerInfos: signerInfos,
			}
			if txFull.AuthInfo.Tip != nil {
				tipAmount := make([]model.TipAmount, 0)
				for _, a := range txFull.AuthInfo.Tip.Amount {
					tipAmount = append(tipAmount, model.TipAmount{
						Denom:  a.Denom,
						Amount: decimal.NewFromInt(a.Amount.Int64()),
					})
				}
				txAuthInfo.Tip = model.Tip{
					Tipper: txFull.AuthInfo.Tip.Tipper,
					Amount: tipAmount,
				}
			}

			processedTx.Tx.AuthInfo = txAuthInfo
		}

		currTxDbWrappers[txIdx] = processedTx
	}

	return currTxDbWrappers, blockTime, nil
}

// ProcessRPCTXs - Given an RPC response, build out the more specific data used by the parser.
func (a *parser) ProcessRPCTXs(messageTypeFilters []filter.MessageTypeFilter,
	txEventResp *cosmosTx.GetTxsEventResponse,
) ([]dbTypes.TxDBWrapper, *time.Time, error) {
	currTxDbWrappers := make([]dbTypes.TxDBWrapper, len(txEventResp.Txs))
	var blockTime *time.Time

	for txIdx := range txEventResp.Txs {
		// Indexer types only used by the indexer app (similar to the cosmos types)
		var indexerMergedTx txtypes.MergedTx
		var indexerTx txtypes.IndexerTx
		var txBody txtypes.Body
		var currMessages []types.Msg
		var currLogMsgs []txtypes.LogMessage
		var messagesRaw [][]byte

		currTx := txEventResp.Txs[txIdx]
		currTxResp := txEventResp.TxResponses[txIdx]

		// Get the Messages and Message Logs
		for msgIdx := range currTx.Body.Messages {

			shouldIndex, err := a.messageTypeShouldIndex(currTx.Body.Messages[msgIdx].TypeUrl, messageTypeFilters)
			if err != nil {
				return nil, blockTime, err
			}

			if !shouldIndex {
				config.Log.Debug(fmt.Sprintf("[Block: %v] [TX: %v] Skipping msg of type '%v'.", currTxResp.Height, currTxResp.TxHash, currTx.Body.Messages[msgIdx].TypeUrl))
				currMessages = append(currMessages, nil)
				currLogMsgs = append(currLogMsgs, txtypes.LogMessage{
					MessageIndex: msgIdx,
				})
				messagesRaw = append(messagesRaw, nil)
				continue
			}

			currMsg := currTx.Body.Messages[msgIdx].GetCachedValue()
			messagesRaw = append(messagesRaw, currTx.Body.Messages[msgIdx].Value)

			// If we reached here, unpacking the entire TX raw was not successful
			// Attempt to unpack the message individually.
			if currMsg == nil {
				var currMsgUnpack types.Msg

				err = a.cl.Codec.InterfaceRegistry.UnpackAny(currTx.Body.Messages[msgIdx], &currMsgUnpack)
				if err != nil || currMsgUnpack == nil {
					config.Log.Errorf(fmt.Sprintf("tx message could not be processed. "+
						"Unpacking protos failed and CachedValue is not present. "+
						"TX Hash: %s, Msg type: %s, Msg index: %d, Code: %d, Error: %s. Ignoring....",
						currTxResp.TxHash,
						currTx.Body.Messages[msgIdx].TypeUrl,
						msgIdx,
						currTxResp.Code,
						err.Error(),
					))
					continue
					/*
						return nil, blockTime, fmt.Errorf("tx message could not be processed. Unpacking protos failed and CachedValue is not present. TX Hash: %s, Msg type: %s, Msg index: %d, Code: %d",
							currTxResp.TxHash,
							currTx.Body.Messages[msgIdx].TypeUrl,
							msgIdx,
							currTxResp.Code,
						)*/
				}
				currMsg = currMsgUnpack
			}

			if currMsg != nil {
				msg := currMsg.(types.Msg)
				currMessages = append(currMessages, msg)
				if len(currTxResp.Logs) >= msgIdx+1 {
					msgEvents := currTxResp.Logs[msgIdx].Events
					currTxLog := txtypes.LogMessage{
						MessageIndex: msgIdx,
						Events:       a.toEvents(msgEvents),
					}
					currLogMsgs = append(currLogMsgs, currTxLog)
				}
			}
		}

		txBody.Messages = currMessages
		indexerTx.Body = txBody

		indexerTxResp := txtypes.Response{
			TxHash:    currTxResp.TxHash,
			Height:    fmt.Sprintf("%d", currTxResp.Height),
			TimeStamp: currTxResp.Timestamp,
			RawLog:    []byte(currTxResp.RawLog),
			Log:       currLogMsgs,
			Code:      currTxResp.Code,
			GasUsed:   currTxResp.GasUsed,
			GasWanted: currTxResp.GasWanted,
			Info:      currTxResp.Info,
			Data:      currTxResp.Data,
		}

		indexerTx.AuthInfo = *currTx.AuthInfo
		indexerMergedTx.TxResponse = indexerTxResp
		indexerMergedTx.Tx = indexerTx
		indexerMergedTx.Tx.AuthInfo = *currTx.AuthInfo

		processedTx, txTime, err := a.processor.ProcessTx(indexerMergedTx, messagesRaw)
		if err != nil {
			return currTxDbWrappers, blockTime, err
		}

		if blockTime == nil {
			blockTime = &txTime
		}

		filteredSigners := make([]types.AccAddress, 0)
		for _, filteredMessage := range txBody.Messages {
			if filteredMessage != nil {
				filteredSigners = append(filteredSigners, filteredMessage.GetSigners()...)
			}
		}

		err = currTx.AuthInfo.UnpackInterfaces(a.cl.Codec.InterfaceRegistry)
		if err != nil {
			return currTxDbWrappers, blockTime, err
		}

		signers, signerInfos, err := a.processor.ProcessSigners(currTx.AuthInfo, filteredSigners)
		if err != nil {
			return currTxDbWrappers, blockTime, err
		}
		processedTx.Tx.SignerAddresses = signers

		fees, err := a.processor.ProcessFees(indexerTx.AuthInfo, signers)
		if err != nil {
			return currTxDbWrappers, blockTime, err
		}

		processedTx.Tx.Fees = fees

		// extra fields
		processedTx.Tx.Signatures = currTx.Signatures
		processedTx.Tx.Memo = currTx.Body.Memo
		processedTx.Tx.TimeoutHeight = currTx.Body.TimeoutHeight

		extensionOptions := make([]string, 0)
		for _, opt := range currTx.Body.ExtensionOptions {
			extensionOptions = append(extensionOptions, opt.String())
		}
		processedTx.Tx.ExtensionOptions = extensionOptions

		nonExtensionOptions := make([]string, 0)
		for _, opt := range currTx.Body.NonCriticalExtensionOptions {
			extensionOptions = append(extensionOptions, opt.String())
		}
		processedTx.Tx.NonCriticalExtensionOptions = nonExtensionOptions
		processedTx.Tx.TxResponse = model.TxResponse{
			TxHash:    indexerTxResp.TxHash,
			Height:    indexerTxResp.Height,
			TimeStamp: indexerTxResp.TimeStamp,
			Code:      indexerTxResp.Code,
			RawLog:    indexerTxResp.RawLog,
			GasUsed:   indexerTxResp.GasUsed,
			GasWanted: indexerTxResp.GasWanted,
			Codespace: indexerTxResp.Codespace,
		}

		if currTx.AuthInfo != nil {
			txAuthInfo := model.AuthInfo{
				Fee: model.AuthInfoFee{
					Granter:  currTx.AuthInfo.Fee.Granter,
					Payer:    currTx.AuthInfo.Fee.Payer,
					GasLimit: currTx.AuthInfo.Fee.GasLimit,
				},
				SignerInfos: signerInfos,
			}
			if currTx.AuthInfo.Tip != nil {
				tipAmount := make([]model.TipAmount, 0)
				for _, a := range currTx.AuthInfo.Tip.Amount {
					tipAmount = append(tipAmount, model.TipAmount{
						Denom:  a.Denom,
						Amount: decimal.NewFromInt(a.Amount.Int64()),
					})
				}
				txAuthInfo.Tip = model.Tip{
					Tipper: currTx.AuthInfo.Tip.Tipper,
					Amount: tipAmount,
				}
			}

			processedTx.Tx.AuthInfo = txAuthInfo
		}

		currTxDbWrappers[txIdx] = processedTx
	}

	return currTxDbWrappers, blockTime, nil
}

func (a *parser) messageTypeShouldIndex(messageType string, filters []filter.MessageTypeFilter) (bool, error) {
	if len(filters) != 0 {
		filterData := filter.MessageTypeData{
			MessageType: messageType,
		}

		matches := false
		for _, messageTypeFilter := range filters {
			typeMatch, err := messageTypeFilter.MessageTypeMatches(filterData)
			if err != nil {
				return false, err
			}
			if typeMatch {
				matches = true
				break
			}
		}

		return matches, nil
	}

	return true, nil
}

func (a *parser) toAttributes(attrs []types.Attribute) []txtypes.Attribute {
	list := []txtypes.Attribute{}
	for _, attr := range attrs {
		lma := txtypes.Attribute{Key: attr.Key, Value: attr.Value}
		list = append(list, lma)
	}

	return list
}

func (a *parser) toEvents(msgEvents types.StringEvents) (list []txtypes.LogMessageEvent) {
	for _, evt := range msgEvents {
		lme := txtypes.LogMessageEvent{Type: evt.Type, Attributes: a.toAttributes(evt.Attributes)}
		list = append(list, lme)
	}

	return list
}

func (a *parser) getUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func tendermintHashToHex(hash []byte) string {
	return strings.ToUpper(hex.EncodeToString(hash))
}

// Unmarshal JSON to a particular type. There can be more than one handler for each type.
// TODO: Remove this map and replace with a more generic solution
var messageTypeHandler = map[string][]func() txtypes.CosmosMessage{}

// var messageTypeIgnorer = map[string]interface{}{}

// Merge the chain specific message type handlers into the core message type handler map.
// Chain specific handlers will be registered BEFORE any generic handlers.
// TODO: Remove this function and replace with a more generic solution
func ChainSpecificMessageTypeHandlerBootstrap(chainID string) {
	var chainSpecificMessageTpeHandler map[string][]func() txtypes.CosmosMessage
	for key, value := range chainSpecificMessageTpeHandler {
		if list, ok := messageTypeHandler[key]; ok {
			messageTypeHandler[key] = append(value, list...)
		} else {
			messageTypeHandler[key] = value
		}
	}
}
