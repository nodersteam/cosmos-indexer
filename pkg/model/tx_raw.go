package model

import (
	"time"

	"github.com/shopspring/decimal"
)

type Transaction struct {
	Messages []string
	Memo     string
	AuthInfo TxAuthInfo
}

type TxAuthInfo struct {
	PublicKeys []string
	Fee        TxFee
	Signatures []string
}

type TxFee struct {
	Amount   Denom
	GasLimit string
	Payer    string
	Granter  string
}

type Denom struct {
	Denom  string
	Amount string
}

type TotalTransactions struct {
	Total     int64           `json:"total"`
	Total24H  int64           `json:"total_24h"`
	Total48H  int64           `json:"total_48h"`
	Total30D  int64           `json:"total_30d"`
	Volume24H decimal.Decimal `json:"volume_24h"`
	Volume30D decimal.Decimal `json:"volume_30d"`
}

type TxSenderReceiver struct {
	MessageType string `json:"message_type,omitempty"`
	Sender      string `json:"sender,omitempty"`
	Receiver    string `json:"receiver,omitempty"`
	Amount      string `json:"amount,omitempty"`
	Denom       string `json:"denom,omitempty"`
}

type VotesTransaction struct {
	BlockHeight int64     `json:"block_height"`
	Timestamp   time.Time `json:"timestamp"`
	TxHash      string    `json:"tx_hash"`
	ProposalID  int       `json:"proposal_id"`
	Voter       string    `json:"voter"`
	Option      string    `json:"option"`
	Weight      string    `json:"weight"`
	Tx          *Tx       `json:"tx"`
}

type TxEvents struct {
	MessageType string `json:"message_type,omitempty"`
	EventIndex  int    `json:"event_index"`
	Type        string `json:"type,omitempty"`
	Index       int    `json:"index,omitempty"`
	Value       string `json:"value,omitempty"`
	Key         string `json:"key,omitempty"`
}

type WalletWithTxs struct {
	Account string `json:"account"`
	TxCount int64  `json:"tx_count"`
}

type DecCoin struct {
	Denom  string
	Amount decimal.Decimal
}

type AccountInfo struct {
	TotalTransactions    int64
	FirstTransactionDate time.Time
	TotalReceived        DecCoin
	TotalSpent           DecCoin
}

type SortBy struct {
	By        string
	Direction string
}

type ProposalDeposit struct {
	Address string
	TxHash  string
	TxTime  time.Time
	Amount  DecCoin
}
