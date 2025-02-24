package model

import (
	"time"

	"github.com/noders-team/cosmos-indexer/db/models"

	"github.com/lib/pq"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Tx struct {
	ID                          uint
	Hash                        string `gorm:"uniqueIndex"`
	Code                        uint32
	BlockID                     uint
	Block                       models.Block
	SignerAddresses             []models.Address `gorm:"many2many:tx_signer_addresses;"`
	Fees                        []Fee
	Signatures                  pq.ByteaArray `gorm:"type:bytea[]" json:"signatures"`
	Timestamp                   time.Time
	Memo                        string
	TimeoutHeight               uint64
	ExtensionOptions            pq.StringArray `gorm:"type:text[]" json:"extension_options"`
	NonCriticalExtensionOptions pq.StringArray `gorm:"type:text[]" json:"non_critical_options"`
	AuthInfoID                  uint
	AuthInfo                    AuthInfo `gorm:"foreignKey:AuthInfoID;belongsTo"`
	TxResponseID                uint
	TxResponse                  TxResponse        `gorm:"foreignKey:TxResponseID;belongsTo"`
	SenderReceiver              *TxSenderReceiver `gorm:"-" json:"sender_receiver"`
	Events                      []*TxEvents       `gorm:"-" json:"events"`
}

type AuthInfo struct {
	ID          uint `gorm:"primarykey"`
	FeeID       uint
	Fee         AuthInfoFee `gorm:"foreignKey:FeeID"`
	TipID       uint
	Tip         Tip           `gorm:"foreignKey:TipID"`
	SignerInfos []*SignerInfo `gorm:"many2many:tx_signer_infos;"`
}

func (AuthInfo) TableName() string {
	return "tx_auth_info"
}

type AuthInfoFee struct {
	ID       uint
	GasLimit uint64
	Payer    string
	Granter  string
	// Amount   []InfoFeeAmount `gorm:"many2many:tx_info_fee_amount;"`
}

func (AuthInfoFee) TableName() string {
	return "tx_auth_info_fee"
}

type InfoFeeAmount struct {
	ID     uint            `gorm:"primaryKey"`
	Amount decimal.Decimal `gorm:"type:decimal(78,0);"`
	Denom  string
}

func (InfoFeeAmount) TableName() string {
	return "tx_info_fee_amount"
}

type Tip struct {
	ID     uint
	Tipper string
	Amount []TipAmount `gorm:"foreignKey:ID"`
}

func (Tip) TableName() string {
	return "tx_tip"
}

type TipAmount struct {
	ID     uint            `gorm:"primaryKey"`
	Amount decimal.Decimal `gorm:"type:decimal(78,0);"`
	Denom  string
}

func (TipAmount) TableName() string {
	return "tx_tip_amount"
}

type SignerInfo struct {
	ID        uint
	AddressID uint
	Address   *models.Address `gorm:"foreignKey:AddressID"`
	ModeInfo  string
	Sequence  uint64
}

func (SignerInfo) TableName() string {
	return "tx_signer_info"
}

type TxResponse struct {
	ID        uint
	TxHash    string `gorm:"uniqueIndex"`
	Height    string
	TimeStamp string
	Code      uint32
	RawLog    []byte `gorm:"type:bytea"`
	// Log       []LogMessage
	GasUsed   int64
	GasWanted int64
	Codespace string
	Data      string
	Info      string
}

func (TxResponse) TableName() string {
	return "tx_responses"
}

type FailedTx struct {
	ID      uint
	Hash    string `gorm:"uniqueIndex"`
	BlockID uint
	Block   models.Block
}

type Fee struct {
	ID             uint            `gorm:"primaryKey"`
	TxID           uint            `gorm:"uniqueIndex:txDenomFee"`
	Amount         decimal.Decimal `gorm:"type:decimal(78,0);"`
	DenominationID uint            `gorm:"uniqueIndex:txDenomFee"`
	Denomination   models.Denom    `gorm:"foreignKey:DenominationID"`
	PayerAddressID uint            `gorm:"index:idx_payer_addr"`
	PayerAddress   models.Address  `gorm:"foreignKey:PayerAddressID"`
}

// BeforeCreate
// This lifecycle function ensures the on conflict statement is added for Fees which are associated to Txes by the Gorm slice association method for has_many
func (b *Fee) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_id"}, {Name: "denomination_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"amount"}),
	})
	return nil
}

type MessageType struct {
	ID          uint   `gorm:"primaryKey"`
	MessageType string `gorm:"uniqueIndex;not null"`
}

type Message struct {
	ID            uint
	TxID          uint `gorm:"uniqueIndex:messageIndex,priority:1"`
	Tx            Tx
	MessageTypeID uint `gorm:"foreignKey:MessageTypeID,index:idx_txid_typeid"`
	MessageType   MessageType
	MessageIndex  int `gorm:"uniqueIndex:messageIndex,priority:2"`
	MessageBytes  []byte
}

type FailedMessage struct {
	ID           uint
	MessageIndex int
	TxID         uint
	Tx           Tx
}

type MessageEvent struct {
	ID uint
	// These fields uniquely identify every message event
	// Index refers to the position of the event in the message event array
	Index              uint64 `gorm:"uniqueIndex:messageEventIndex,priority:2"`
	MessageID          uint   `gorm:"uniqueIndex:messageEventIndex,priority:1"`
	Message            Message
	MessageEventTypeID uint
	MessageEventType   MessageEventType
}

type MessageEventType struct {
	ID   uint
	Type string `gorm:"uniqueIndex"`
}

type MessageEventAttribute struct {
	ID             uint
	MessageEvent   MessageEvent
	MessageEventID uint `gorm:"uniqueIndex:messageAttributeIndex,priority:1"`
	Value          string
	Index          uint64 `gorm:"uniqueIndex:messageAttributeIndex,priority:2"`
	// Keys are limited to a smallish subset of string values set by the Cosmos SDK and external modules
	// Save DB space by storing the key as a foreign key
	MessageEventAttributeKeyID uint
	MessageEventAttributeKey   MessageEventAttributeKey
}

type MessageEventAttributeKey struct {
	ID  uint
	Key string `gorm:"uniqueIndex"`
}

type TxDelegateAggregated struct {
	ID          uint
	Hash        string `gorm:"uniqueIndex"`
	TxType      string
	Timestamp   time.Time
	Validator   string
	BlockHeight int64
	Amount      decimal.Decimal `gorm:"type:decimal(78,0);"`
	Denom       string
	Sender      string
}

type TxEventsValsAggregated struct {
	ID          uint
	EvAttrValue string `gorm:"uniqueIndex:txEventsValsAggregatedIndex"`
	MsgType     string `gorm:"uniqueIndex:txEventsValsAggregatedIndex"`
	TxHash      string `gorm:"uniqueIndex:txEventsValsAggregatedIndex"`
	TxTimestamp time.Time
}

type TxEventsAggregated struct {
	ID                    uint
	TxHash                string `gorm:"uniqueIndex:txEventsAggregatedIndex"`
	MessageType           string `gorm:"uniqueIndex:txEventsAggregatedIndex"`
	MessageTypeIndex      int    `gorm:"uniqueIndex:txEventsAggregatedIndex"`
	MessageEventType      string `gorm:"uniqueIndex:txEventsAggregatedIndex"`
	MessageEventAttrIndex int    `gorm:"uniqueIndex:txEventsAggregatedIndex"`
	MessageEventAttrValue string
	MessageEventAttrKey   string `gorm:"uniqueIndex:txEventsAggregatedIndex"`
}
