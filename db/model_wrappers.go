package db

import (
	"github.com/nodersteam/cosmos-indexer/db/models"
	"github.com/nodersteam/cosmos-indexer/parsers"
)

type BlockDBWrapper struct {
	Block                         *models.Block
	BeginBlockEvents              []BlockEventDBWrapper
	EndBlockEvents                []BlockEventDBWrapper
	UniqueBlockEventTypes         map[string]models.BlockEventType
	UniqueBlockEventAttributeKeys map[string]models.BlockEventAttributeKey
}

type BlockEventDBWrapper struct {
	BlockEvent               models.BlockEvent
	Attributes               []models.BlockEventAttribute
	BlockEventParsedDatasets []parsers.BlockEventParsedData
}

type TxDBWrapper struct {
	Tx                         models.Tx
	Messages                   []MessageDBWrapper
	UniqueMessageTypes         map[string]models.MessageType
	UniqueMessageEventTypes    map[string]models.MessageEventType
	UniqueMessageAttributeKeys map[string]models.MessageEventAttributeKey
}

type MessageDBWrapper struct {
	Message       models.Message
	MessageEvents []MessageEventDBWrapper
}

type MessageEventDBWrapper struct {
	MessageEvent models.MessageEvent
	Attributes   []models.MessageEventAttribute
}

type DenomDBWrapper struct {
	Denom models.Denom
}
