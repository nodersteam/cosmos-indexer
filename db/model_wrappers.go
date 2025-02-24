package db

import (
	"github.com/noders-team/cosmos-indexer/db/models"
	"github.com/noders-team/cosmos-indexer/parsers"
	"github.com/noders-team/cosmos-indexer/pkg/model"
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
	Tx                         model.Tx
	Messages                   []MessageDBWrapper
	UniqueMessageTypes         map[string]model.MessageType
	UniqueMessageEventTypes    map[string]model.MessageEventType
	UniqueMessageAttributeKeys map[string]model.MessageEventAttributeKey
}

type MessageDBWrapper struct {
	Message       model.Message
	MessageEvents []MessageEventDBWrapper
}

type MessageEventDBWrapper struct {
	MessageEvent model.MessageEvent
	Attributes   []model.MessageEventAttribute
}

type DenomDBWrapper struct {
	Denom models.Denom
}
