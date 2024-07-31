package tx

import (
	"fmt"
	"github.com/DefiantLabs/probe/client"
	"github.com/cosmos/cosmos-sdk/crypto/keys/multisig"
	cryptoTypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/cosmos/cosmos-sdk/types"
	cosmosTx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/nodersteam/cosmos-indexer/config"
	txtypes "github.com/nodersteam/cosmos-indexer/cosmos/modules/tx"
	dbTypes "github.com/nodersteam/cosmos-indexer/db"
	"github.com/nodersteam/cosmos-indexer/db/models"
	"github.com/nodersteam/cosmos-indexer/util"
	"math/big"
	"time"
)

type Processor interface {
	ProcessTx(tx txtypes.MergedTx, messagesRaw [][]byte) (txDBWapper dbTypes.TxDBWrapper,
		txTime time.Time, err error)
	ProcessSigners(authInfo *cosmosTx.AuthInfo,
		messageSigners []types.AccAddress) ([]models.Address, []*models.SignerInfo, error)
	ProcessFees(authInfo cosmosTx.AuthInfo, signers []models.Address) ([]models.Fee, error)
	ProcessMessage(messageIndex int, message types.Msg,
		txMessageEventLogs []txtypes.LogMessage,
		uniqueEventTypes map[string]models.MessageEventType,
		uniqueEventAttributeKeys map[string]models.MessageEventAttributeKey) (string, dbTypes.MessageDBWrapper)
}

type processor struct {
	cl *client.ChainClient
}

func NewProcessor(cl *client.ChainClient) Processor {
	return &processor{cl: cl}
}

func (a *processor) ProcessTx(tx txtypes.MergedTx, messagesRaw [][]byte) (txDBWapper dbTypes.TxDBWrapper, txTime time.Time, err error) {
	txTime, err = time.Parse(time.RFC3339, tx.TxResponse.TimeStamp)
	if err != nil {
		config.Log.Error("Error parsing tx timestamp.", err)
		return txDBWapper, txTime, err
	}

	code := tx.TxResponse.Code

	var messages []dbTypes.MessageDBWrapper

	uniqueMessageTypes := make(map[string]models.MessageType)
	uniqueEventTypes := make(map[string]models.MessageEventType)
	uniqueEventAttributeKeys := make(map[string]models.MessageEventAttributeKey)
	// non-zero code means the Tx was unsuccessful. We will still need to account for fees in both cases though.
	if code == 0 {
		for messageIndex, message := range tx.Tx.Body.Messages {
			if message != nil {
				messageType, currMessageDBWrapper := a.ProcessMessage(messageIndex, message,
					tx.TxResponse.Log, uniqueEventTypes, uniqueEventAttributeKeys)
				currMessageDBWrapper.Message.MessageBytes = messagesRaw[messageIndex]
				uniqueMessageTypes[messageType] = currMessageDBWrapper.Message.MessageType
				config.Log.Debug(fmt.Sprintf("[Block: %v] [TX: %v] Found msg of type '%v'.", tx.TxResponse.Height, tx.TxResponse.TxHash, messageType))
				messages = append(messages, currMessageDBWrapper)
			}
		}
	}

	txDBWapper.Tx = models.Tx{Hash: tx.TxResponse.TxHash, Code: code, Timestamp: txTime}
	txDBWapper.Messages = messages
	txDBWapper.UniqueMessageTypes = uniqueMessageTypes
	txDBWapper.UniqueMessageAttributeKeys = uniqueEventAttributeKeys
	txDBWapper.UniqueMessageEventTypes = uniqueEventTypes

	return txDBWapper, txTime, nil
}

// ProcessSigners in a deterministic order.
// 1. Processes signers from the auth info
// 2. Processes signers from the signers array
// 3. Processes the fee payer
func (a *processor) ProcessSigners(authInfo *cosmosTx.AuthInfo,
	messageSigners []types.AccAddress) ([]models.Address, []*models.SignerInfo, error) {
	// For unique checks
	signerAddressMap := make(map[string]models.Address)
	// For deterministic output of signer values
	var signerAddressArray []models.Address
	signerInfos := make([]*models.SignerInfo, 0)

	// If there is a signer info, get the addresses from the keys add it to the list of signers
	for _, signerInfo := range authInfo.SignerInfos {
		if signerInfo.PublicKey != nil {
			var info models.SignerInfo

			pubKey, err := a.cl.Codec.InterfaceRegistry.Resolve(signerInfo.PublicKey.TypeUrl)
			if err != nil {
				config.Log.Error("ProcessSigners cl.Codec.InterfaceRegistry.Resolve ", err)
				return nil, nil, err
			}

			err = a.cl.Codec.InterfaceRegistry.UnpackAny(signerInfo.PublicKey, &pubKey)
			if err != nil {
				config.Log.Error("ProcessSigners cl.Codec.InterfaceRegistry.UnpackAny ", err)
				return nil, nil, err
			}

			multisigKey, ok := pubKey.(*multisig.LegacyAminoPubKey)

			if ok {
				for _, key := range multisigKey.GetPubKeys() {
					address := types.AccAddress(key.Address().Bytes()).String()
					if _, ok := signerAddressMap[address]; !ok {
						signerAddressArray = append(signerAddressArray, models.Address{Address: address})
					}
					signerAddr := models.Address{Address: address}
					signerAddressMap[address] = signerAddr
					info.Address = &signerAddr
				}
			} else {
				castPubKey, ok := pubKey.(cryptoTypes.PubKey)
				if !ok {
					return nil, nil, err
				}

				address := types.AccAddress(castPubKey.Address().Bytes()).String()
				if _, ok := signerAddressMap[address]; !ok {
					signerAddressArray = append(signerAddressArray, models.Address{Address: address})
				}
				signerAddr := models.Address{Address: address}
				signerAddressMap[address] = signerAddr
				info.Address = &signerAddr
			}

			info.Sequence = signerInfo.Sequence
			info.ModeInfo = signerInfo.ModeInfo.String()
			signerInfos = append(signerInfos, &info)
		}
	}

	for _, signer := range messageSigners {
		addressStr := signer.String()
		if _, ok := signerAddressMap[addressStr]; !ok {
			signerAddressArray = append(signerAddressArray, models.Address{Address: addressStr})
		}
		signerAddressMap[addressStr] = models.Address{Address: addressStr}
	}

	// If there is a fee payer, add it to the list of signers
	if authInfo.Fee.GetPayer() != "" {
		if _, ok := signerAddressMap[authInfo.Fee.GetPayer()]; !ok {
			signerAddressArray = append(signerAddressArray, models.Address{Address: authInfo.Fee.GetPayer()})
		}
		signerAddressMap[authInfo.Fee.GetPayer()] = models.Address{Address: authInfo.Fee.GetPayer()}
	}

	return signerAddressArray, signerInfos, nil
}

// ProcessFees Processes fees into model form, applying denoms and addresses to them
func (a *processor) ProcessFees(authInfo cosmosTx.AuthInfo, signers []models.Address) ([]models.Fee, error) {
	// TODO not the best way
	if authInfo.Fee == nil {
		fees := make([]models.Fee, 0)
		return fees, nil
	}

	feeCoins := authInfo.Fee.Amount
	payer := authInfo.Fee.GetPayer()
	fees := make([]models.Fee, 0)

	for _, coin := range feeCoins {
		zeroFee := big.NewInt(0)

		if zeroFee.Cmp(coin.Amount.BigInt()) != 0 {
			amount := util.ToNumeric(coin.Amount.BigInt())
			denom := models.Denom{Base: coin.Denom}

			payerAddr := models.Address{}
			if payer != "" {
				payerAddr.Address = payer
			} else if len(signers) > 0 {
				payerAddr = signers[0]
			}

			fees = append(fees, models.Fee{Amount: amount, Denomination: denom, PayerAddress: payerAddr})
		}
	}

	return fees, nil
}

func (a *processor) ProcessMessage(messageIndex int, message types.Msg,
	txMessageEventLogs []txtypes.LogMessage,
	uniqueEventTypes map[string]models.MessageEventType,
	uniqueEventAttributeKeys map[string]models.MessageEventAttributeKey) (string, dbTypes.MessageDBWrapper) {
	var currMessage models.Message
	var currMessageType models.MessageType
	currMessage.MessageIndex = messageIndex

	// Get the message log that corresponds to the current message
	var currMessageDBWrapper dbTypes.MessageDBWrapper
	messageLog := txtypes.GetMessageLogForIndex(txMessageEventLogs, messageIndex)

	currMessageType.MessageType = types.MsgTypeURL(message)
	currMessage.MessageType = currMessageType
	currMessageDBWrapper.Message = currMessage

	for eventIndex, event := range messageLog.Events {
		uniqueEventTypes[event.Type] = models.MessageEventType{Type: event.Type}

		var currMessageEvent dbTypes.MessageEventDBWrapper
		currMessageEvent.MessageEvent = models.MessageEvent{
			MessageEventType: uniqueEventTypes[event.Type],
			Index:            uint64(eventIndex),
		}
		var currMessageEventAttributes []models.MessageEventAttribute
		for attributeIndex, attribute := range event.Attributes {
			uniqueEventAttributeKeys[attribute.Key] = models.MessageEventAttributeKey{Key: attribute.Key}

			currMessageEventAttributes = append(currMessageEventAttributes, models.MessageEventAttribute{
				Value:                    attribute.Value,
				MessageEventAttributeKey: uniqueEventAttributeKeys[attribute.Key],
				Index:                    uint64(attributeIndex),
			})
		}

		currMessageEvent.Attributes = currMessageEventAttributes
		currMessageDBWrapper.MessageEvents = append(currMessageDBWrapper.MessageEvents, currMessageEvent)
	}
	return currMessageType.MessageType, currMessageDBWrapper
}
