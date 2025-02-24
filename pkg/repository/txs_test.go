package repository

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/noders-team/cosmos-indexer/pkg/model"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/require"
)

const txes = `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $2, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $3, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $4, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (5, 'hash5', 101112, 5, '{"signature7", "signature8"}', $4, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (6, 'hash6', 101112, 5, '{"signature7", "signature8"}', $5, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `

func TestTransactionsPerPeriod(t *testing.T) {
	type expected struct {
		allTx  int64
		all24H int64
		all30D int64
		err    error
	}

	sampleData := `INSERT INTO txes (hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  ('random_hash_1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  ('random_hash_2', 456, 2, '{"signature3", "signature4"}', $1, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  ('random_hash_3', 789, 3, '{"signature5", "signature6"}', $1, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  ('random_hash_4', 101112, 4, '{"signature7", "signature8"}', $1, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  ('random_hash_5', 131415, 5, '{"signature9", "signature10"}', $1, 'Random memo 5', 500, '{"option9", "option10"}', '{"non_critical_option9", "non_critical_option10"}', 5, 5),
									  ('random_hash_6', 161718, 6, '{"signature11", "signature12"}', $1, 'Random memo 6', 600, '{"option11", "option12"}', '{"non_critical_option11", "non_critical_option12"}', 6, 6),
									  ('random_hash_7', 192021, 7, '{"signature13", "signature14"}', $1, 'Random memo 7', 700, '{"option13", "option14"}', '{"non_critical_option13", "non_critical_option14"}', 7, 7),
									  ('random_hash_8', 222324, 8, '{"signature15", "signature16"}', $1, 'Random memo 8', 800, '{"option15", "option16"}', '{"non_critical_option15", "non_critical_option16"}', 8, 8),
									  ('random_hash_9', 252627, 9, '{"signature17", "signature18"}', $1, 'Random memo 9', 900, '{"option17", "option18"}', '{"non_critical_option17", "non_critical_option18"}', 9, 9),
									  ('random_hash_10', 282930, 10, '{"signature19", "signature20"}', $1, 'Random memo 10', 1000, '{"option19", "option20"}', '{"non_critical_option19", "non_critical_option20"}', 10, 10);
			`

	tests := []struct {
		name   string
		before func()
		to     time.Time
		result expected
		after  func()
	}{
		{
			"success",
			func() {
				_, err := postgresConn.Exec(context.Background(), sampleData, time.Now().UTC().Add(-1*time.Hour))
				require.NoError(t, err)
			},
			time.Now().UTC(),
			expected{allTx: 10, all24H: 10, all30D: 10, err: nil},
			func() {
				_, err := postgresConn.Exec(context.Background(), `delete from txes`)
				require.NoError(t, err)
			},
		},
		{
			"success_no24h",
			func() {
				_, err := postgresConn.Exec(context.Background(), sampleData, time.Now().UTC().Add(-25*time.Hour))
				require.NoError(t, err)
			},
			time.Now().UTC(),
			expected{allTx: 10, all24H: 0, all30D: 10, err: nil},
			func() {
				_, err := postgresConn.Exec(context.Background(), `delete from txes`)
				require.NoError(t, err)
			},
		},
		{
			"success_no24h_no30d",
			func() {
				_, err := postgresConn.Exec(context.Background(), sampleData, time.Now().UTC().Add(-24*31*time.Hour))
				require.NoError(t, err)
			},
			time.Now().UTC(),
			expected{allTx: 10, all24H: 0, all30D: 0, err: nil},
			func() {
				_, err := postgresConn.Exec(context.Background(), `delete from txes`)
				require.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before()
			txsRepo := NewTxs(postgresConn)
			allTx, all24H, _, all30D, err := txsRepo.TransactionsPerPeriod(context.Background(), tt.to)
			require.Equal(t, tt.result.err, err)
			require.Equal(t, tt.result.allTx, allTx)
			require.Equal(t, tt.result.all24H, all24H)
			require.Equal(t, tt.result.all30D, all30D)
			tt.after()
		})
	}
}

func TestTxs_TransactionRawLog(t *testing.T) {
	ctx := context.Background()

	type expected struct {
		rawLog string
		err    error
	}

	type params struct {
		txHash string
	}

	txResponses := `
					INSERT INTO tx_responses (id, tx_hash, height, time_stamp, code, raw_log, gas_used, gas_wanted, codespace, data, info)
					VALUES 
					  (1, 'hash1', '1234', '2024-04-22 12:00:00', 0, 'raw_log_1', 100, 200, 'codespace1', 'data1', 'info1'),
					  (2, 'hash2', '1235', '2024-04-23 12:00:00', 1, 'raw_log_2', 150, 250, 'codespace2', 'data2', 'info2'),
					  (3, 'hash3', '1236', '2024-04-24 12:00:00', 2, 'raw_log_3', 200, 300, 'codespace3', 'data3', 'info3');
					`
	txes := `INSERT INTO txes (hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  ('hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  ('hash2', 456, 2, '{"signature3", "signature4"}', $1, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  ('hash3', 789, 3, '{"signature5", "signature6"}', $1, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  ('hash4', 101112, 4, '{"signature7", "signature8"}', $1, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	tests := []struct {
		name     string
		expected expected
		params   params
		before   func()
		after    func()
	}{
		{
			"success",
			expected{rawLog: "raw_log_1", err: nil},
			params{"hash1"},
			func() {
				_, err := postgresConn.Exec(ctx, txResponses)
				require.NoError(t, err)
				_, err = postgresConn.Exec(ctx, txes, time.Now().UTC())
				require.NoError(t, err)
			},
			func() {
				_, err := postgresConn.Exec(context.Background(), `delete from txes`)
				require.NoError(t, err)
				_, err = postgresConn.Exec(context.Background(), `delete from tx_responses`)
				require.NoError(t, err)
			},
		},
		{
			"not_found",
			expected{err: fmt.Errorf("not found")},
			params{"hash7"},
			func() {
				_, err := postgresConn.Exec(ctx, txResponses)
				require.NoError(t, err)
				_, err = postgresConn.Exec(ctx, txes, time.Now().UTC())
				require.NoError(t, err)
			},
			func() {
				_, err := postgresConn.Exec(context.Background(), `delete from txes`)
				require.NoError(t, err)
				_, err = postgresConn.Exec(context.Background(), `delete from tx_responses`)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before()
			txsRepo := NewTxs(postgresConn)
			res, err := txsRepo.TransactionRawLog(context.Background(), tt.params.txHash)
			require.Equal(t, tt.expected.err, err)
			if err == nil {
				require.Equal(t, tt.expected.rawLog, string(res))
			}
			tt.after()
		})
	}
}

func TestTxs_TransactionSigners(t *testing.T) {
	authInfoDemo := `INSERT INTO tx_auth_info (id, fee_id, tip_id) values (1, 1, 1)`
	_, err := postgresConn.Exec(context.Background(), authInfoDemo)
	require.NoError(t, err)

	signerInfos := `INSERT INTO tx_signer_infos (auth_info_id, signer_info_id) values (1, 2)`
	_, err = postgresConn.Exec(context.Background(), signerInfos)
	require.NoError(t, err)

	signerInfo := `INSERT INTO tx_signer_info (id, address_id) values (2, 4)`
	_, err = postgresConn.Exec(context.Background(), signerInfo)
	require.NoError(t, err)

	addresses := `INSERT INTO addresses (id, address) values (4, 'test')`
	_, err = postgresConn.Exec(context.Background(), addresses)
	require.NoError(t, err)

	//nolint:goconst
	demoTransactions := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $1, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $1, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $1, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	_, err = postgresConn.Exec(context.Background(), demoTransactions, time.Now().UTC())
	require.NoError(t, err)

	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from addresses`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from tx_signer_addresses`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from tx_signer_info`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from tx_signer_infos`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from tx_auth_info`)
		require.NoError(t, err)
	}()

	txsRepo := NewTxs(postgresConn)
	res, err := txsRepo.TransactionSigners(context.Background(), "hash1")
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.NotNil(t, res[0].Address)
	require.Equal(t, res[0].Address.Address, "test")
}

func TestTxs_Transactions_ByHash(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
	}()

	txes := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $1, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $1, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $1, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	_, err := postgresConn.Exec(context.Background(), txes, time.Now().UTC())
	require.NoError(t, err)
	txsRepo := NewTxs(postgresConn)
	txHash := "hash1"

	res, _, err := txsRepo.Transactions(context.Background(), 100, 0, &TxsFilter{TxHash: &txHash})
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestTxs_ChartTransactionsByHour(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
	}()

	initTime := time.Now().UTC()
	_, err := postgresConn.Exec(context.Background(), txes,
		initTime,
		initTime.Add(-1*time.Hour),
		initTime.Add(-2*time.Hour),
		initTime.Add(-3*time.Hour),
		initTime.Add(-35*time.Hour))
	require.NoError(t, err)
	txsRepo := NewTxs(postgresConn)
	res, err := txsRepo.ChartTransactionsByHour(context.Background(), initTime.Add(5*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, res.Total24H, int64(5))
	require.Equal(t, res.Total48H, int64(1))
	require.Len(t, res.Points, 4)
}

func TestTxs_ChartTransactionsVolume(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from fees`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from denoms`)
		require.NoError(t, err)
	}()

	batch := pgx.Batch{}

	txes := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $2, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $3, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $4, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (5, 'hash5', 101112, 5, '{"signature7", "signature8"}', $4, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (6, 'hash6', 101112, 5, '{"signature7", "signature8"}', $5, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	initTime := time.Now().UTC()
	batch.Queue(txes, initTime,
		initTime.Add(-1*time.Hour),
		initTime.Add(-2*time.Hour),
		initTime.Add(-3*time.Hour),
		initTime.Add(-35*time.Hour))

	denoms := `INSERT INTO denoms(id, base) VALUES (1, 'utia')`
	batch.Queue(denoms)

	fees := `INSERT INTO fees(id, tx_id, amount, denomination_id)
							VALUES 
								(1, 1, 1000, 1),
								(2, 2, 2000, 1),
								(3, 3, 100, 1),
								(4, 4, 2000, 1),
								(5, 5, 9, 1),
								(6, 6, 27, 1)
							`
	batch.Queue(fees)
	res := postgresConn.SendBatch(context.Background(), &batch)
	defer func(res pgx.BatchResults) {
		err := res.Close()
		require.NoError(t, err)
	}(res)
	for i := 0; i < batch.Len(); i++ {
		_, err := res.Exec()
		if err != nil {
			require.NoError(t, err)
		}
	}

	txsRepo := NewTxs(postgresConn)
	data, err := txsRepo.ChartTransactionsVolume(context.Background(), initTime.Add(5*time.Minute))
	require.NoError(t, err)
	require.Len(t, data, 4)

	require.Equal(t, data[0].TxVolume, decimal.RequireFromString("2009"))
	require.Equal(t, data[1].TxVolume, decimal.RequireFromString("100"))
	require.Equal(t, data[2].TxVolume, decimal.RequireFromString("2000"))
	require.Equal(t, data[3].TxVolume, decimal.RequireFromString("1000"))
}

func TestTxs_ExtractNumber(t *testing.T) {
	txsRepo := NewTxs(postgresConn)
	amount, denom, err := txsRepo.ExtractNumber("18000000utia")
	require.NoError(t, err)
	require.Equal(t, denom, "utia")
	require.Equal(t, amount.String(), "18000000")

	amount, denom, err = txsRepo.ExtractNumber("18000000")
	require.NoError(t, err)
	require.Equal(t, denom, "")
	require.Equal(t, amount.String(), "18000000")

	config := sdk.GetConfig()
	config.SetBech32PrefixForAccount("celestia", "celestiapub")
	config.SetBech32PrefixForValidator("celestiavaloper", "celestiavaloperpub")
	config.SetBech32PrefixForConsensusNode("celestiavalcons", "celestiavalconspub")
	config.Seal()
	valAddr, _ := sdk.ValAddressFromBech32("celestiavaloper1lm4jtr6wjwpamz2e9wlgzdazly3vnwqy53t5t4")
	accAddr, _ := sdk.AccAddressFromHexUnsafe(hex.EncodeToString(valAddr.Bytes()))
	fmt.Println(accAddr.String())
}

func TestTxs_VolumePerPeriod(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from fees`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from denoms`)
		require.NoError(t, err)
	}()

	batch := pgx.Batch{}

	txes := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $2, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $3, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $4, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (5, 'hash5', 101112, 5, '{"signature7", "signature8"}', $4, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4),
									  (6, 'hash6', 101112, 5, '{"signature7", "signature8"}', $5, 'Random memo 5', 600, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	initTime := time.Now().UTC()
	batch.Queue(txes, initTime,
		initTime.Add(-1*time.Hour),
		initTime.Add(-2*time.Hour),
		initTime.Add(-3*time.Hour),
		initTime.Add(-35*time.Hour))

	denoms := `INSERT INTO denoms(id, base) VALUES (1, 'utia')`
	batch.Queue(denoms)

	fees := `INSERT INTO fees(id, tx_id, amount, denomination_id)
							VALUES 
								(1, 1, 1000, 1),
								(2, 2, 2000, 1),
								(3, 3, 100, 1),
								(4, 4, 2000, 1),
								(5, 5, 9, 1),
								(6, 6, 27, 1)
							`
	batch.Queue(fees)
	res := postgresConn.SendBatch(context.Background(), &batch)
	defer func(res pgx.BatchResults) {
		err := res.Close()
		require.NoError(t, err)
	}(res)
	for i := 0; i < batch.Len(); i++ {
		_, err := res.Exec()
		if err != nil {
			require.NoError(t, err)
		}
	}

	txsRepo := NewTxs(postgresConn)
	total24H, total30D, err := txsRepo.VolumePerPeriod(context.Background(),
		initTime.Add(5*time.Minute))
	require.NoError(t, err)
	require.Equal(t, total24H, decimal.RequireFromString("5109"))
	require.Equal(t, total30D, decimal.RequireFromString("5136"))
}

func TestTxs_TransactionsByEventValue(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from message_types`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from message_event_types`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from messages`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from message_events`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from message_event_attribute_keys`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from message_event_attributes`)
		require.NoError(t, err)
	}()

	txes := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height,
			                  extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
												VALUES
												  (100, 'hash1', 123, 1, '{"signature1", "signature2"}', $1,
												   'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
												  (101, 'hash2', 456, 2, '{"signature3", "signature4"}', $2,
												   'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
												  (102, 'hash3', 789, 3, '{"signature5", "signature6"}', $3,
												   'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3);
												  `
	msgMessageType := `
INSERT INTO message_types(id, message_type) values 
                                                (10, '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'), 
                                                (11, '/cosmos.bank.v1beta1.MsgSend'), 
                                                (12, '/cosmos.gov.v1.MsgVote');
`

	msgMessageEventTypes := `
INSERT INTO message_event_types(id, type) values (20, 'proposal_vote'),  (21, 'transfer'), (22, 'coin_spent'), (23, 'coin_received');
`
	msgMessages := `
INSERT INTO messages(id, tx_id, message_type_id, message_index) 
values (30, 100, 10, 1),
(31, 101, 10, 2),
(32, 101, 11, 3),
(33, 102, 12, 4);
`
	msgMessageEvents := `
INSERT INTO message_events(id, index, message_id, message_event_type_id) values (40, 1, 30, 21),
(41, 1, 31, 23),
(42, 2, 32, 22),
(43, 1, 33, 20);
`
	msgMessageEventAtrributesKeys := `
INSERT INTO message_event_attribute_keys(id, key) values (50, 'voter'),
(51, 'receiver'),
(53, 'sender'),
(54, 'proposal_id'),
(55, 'delegator');
`
	msgMessageEventsAttributes := `
INSERT INTO message_event_attributes(id, message_event_id, value, index, message_event_attribute_key_id)
	values 
	    (60, 40, 'celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh', 1, 55),
		(61, 41, 'celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh', 2, 51),
		(62, 42, 'celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh', 3, 53),
		(63, 43, 'celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh', 4, 50),
		(64, 43, '2', 5, 54);
`
	initTime := time.Now().UTC()
	batch := &pgx.Batch{}
	batch.Queue(txes, initTime, initTime.Add(-1*time.Hour), initTime.Add(-2*time.Hour))
	batch.Queue(msgMessageType)
	batch.Queue(msgMessageEventTypes)
	batch.Queue(msgMessages)
	batch.Queue(msgMessageEvents)
	batch.Queue(msgMessageEventAtrributesKeys)
	batch.Queue(msgMessageEventsAttributes)

	res := postgresConn.SendBatch(context.Background(), batch)
	defer func(res pgx.BatchResults) {
		err := res.Close()
		require.NoError(t, err)
	}(res)

	for i := 0; i < batch.Len(); i++ {
		_, err := res.Exec()
		if err != nil {
			log.Info().Msgf("error processing batch %d", i)
			require.NoError(t, err)
		}
	}

	type params struct {
		values   []string
		msgTypes []string
		limit    int64
		offset   int64
	}

	type expected struct {
		total    int64
		resTotal int
	}

	tests := []struct {
		name     string
		request  params
		response expected
	}{
		{
			"success",
			params{
				values:   []string{"2"},
				msgTypes: []string{"/cosmos.gov.v1.MsgVote"},
				limit:    10,
				offset:   0,
			},
			expected{
				1,
				1,
			},
		},
		{
			"success - multiple types",
			params{
				values: []string{"celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh"},
				msgTypes: []string{
					"/cosmos.bank.v1beta1.MsgSend",
					"/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
				},
				limit:  10,
				offset: 0,
			},
			expected{
				2,
				2,
			},
		},
		{
			"success - multiple types, limits",
			params{
				values: []string{"celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh"},
				msgTypes: []string{
					"/cosmos.bank.v1beta1.MsgSend",
					"/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
				},
				limit:  1,
				offset: 0,
			},
			expected{
				2,
				1,
			},
		},
		{
			"success - multiple values",
			params{
				values:   []string{"2", "celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh"},
				msgTypes: []string{"/cosmos.gov.v1.MsgVote"},
				limit:    10,
				offset:   0,
			},
			expected{
				1,
				1,
			},
		},
		{
			"success - not exists",
			params{
				values:   []string{"7", "celestia1v8hn5eu8e2amqq2t2hfu8cv3wknmvxvvsryggh"},
				msgTypes: []string{"/cosmos.gov.v1.MsgVote"},
				limit:    10,
				offset:   0,
			},
			expected{
				0,
				0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txsRepo := NewTxs(postgresConn)
			resTx, total, err := txsRepo.TransactionsByEventValue(context.Background(),
				tt.request.values, tt.request.msgTypes, false, tt.request.limit, tt.request.offset)
			require.NoError(t, err)
			require.Equal(t, total, tt.response.total)
			require.Len(t, resTx, tt.response.resTotal)
		})
	}
}

func TestTxs_GetWalletsCount(t *testing.T) {
	ctx := context.Background()

	type expected struct {
		totalWallets *model.TotalWallets
		err          error
	}

	sampleData := `
		INSERT INTO transactions_normalized (account, time)
		VALUES 
			('account1', $1),
			('account2', $2),
			('account1', $3),
			('account3', $4),
			('account4', $5),
			('account5', $6);
	`

	baseTime := time.Now().UTC()

	tests := []struct {
		name     string
		expected expected
		before   func()
		after    func()
	}{
		{
			name: "success_with_data",
			expected: expected{
				totalWallets: &model.TotalWallets{
					Total:    5,
					Count24H: 1,
					Count48H: 3,
					Count30D: 4,
				},
				err: nil,
			},
			before: func() {
				_, err := postgresConn.Exec(ctx, sampleData,
					baseTime.Add(-25*time.Hour),
					baseTime.Add(-20*time.Hour),
					baseTime.Add(-15*time.Hour),
					baseTime.Add(-10*time.Hour),
					baseTime.Add(-5*time.Hour),
					baseTime.Add(-720*time.Hour), // 30 days ago
				)
				require.NoError(t, err)
			},
			after: func() {
				_, err := postgresConn.Exec(ctx, `DELETE FROM transactions_normalized`)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before()
			txsRepo := NewTxs(postgresConn)
			result, err := txsRepo.GetWalletsCount(ctx)
			if tt.expected.err != nil {
				require.Error(t, err)
				require.Equal(t, tt.expected.err.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected.totalWallets, result)
			tt.after()
		})
	}
}

func TestTxs_GetWalletsCountPerPeriod(t *testing.T) {
	ctx := context.Background()

	type expected struct {
		count int64
		err   error
	}

	type params struct {
		startDate time.Time
		endDate   time.Time
	}

	sampleData := `
		INSERT INTO transactions_normalized (account, time)
		VALUES 
			('account1', $1),
			('account2', $2),
			('account1', $3),
			('account3', $4),
			('account4', $5);
	`

	tests := []struct {
		name     string
		expected expected
		params   params
		before   func()
		after    func()
	}{
		{
			name: "success_all_accounts",
			expected: expected{
				count: 4,
				err:   nil,
			},
			params: params{
				startDate: time.Now().UTC().Add(-24 * time.Hour),
				endDate:   time.Now().UTC(),
			},
			before: func() {
				now := time.Now().UTC()
				_, err := postgresConn.Exec(ctx, sampleData,
					now.Add(-25*time.Hour),
					now.Add(-20*time.Hour),
					now.Add(-15*time.Hour),
					now.Add(-10*time.Hour),
					now.Add(-5*time.Hour),
				)
				require.NoError(t, err)
			},
			after: func() {
				_, err := postgresConn.Exec(ctx, `DELETE FROM transactions_normalized`)
				require.NoError(t, err)
			},
		},
		{
			name: "success_partial_accounts",
			expected: expected{
				count: 3,
				err:   nil,
			},
			params: params{
				startDate: time.Now().UTC().Add(-18 * time.Hour),
				endDate:   time.Now().UTC().Add(-8 * time.Hour),
			},
			before: func() {
				now := time.Now().UTC()
				_, err := postgresConn.Exec(ctx, sampleData,
					now.Add(-25*time.Hour),
					now.Add(-20*time.Hour),
					now.Add(-15*time.Hour),
					now.Add(-10*time.Hour),
					now.Add(-5*time.Hour),
				)
				require.NoError(t, err)
			},
			after: func() {
				_, err := postgresConn.Exec(ctx, `DELETE FROM transactions_normalized`)
				require.NoError(t, err)
			},
		},
		{
			name: "success_no_accounts",
			expected: expected{
				count: 0,
				err:   nil,
			},
			params: params{
				startDate: time.Now().UTC().Add(-50 * time.Hour),
				endDate:   time.Now().UTC().Add(-40 * time.Hour),
			},
			before: func() {
				now := time.Now().UTC()
				_, err := postgresConn.Exec(ctx, sampleData,
					now.Add(-25*time.Hour),
					now.Add(-20*time.Hour),
					now.Add(-15*time.Hour),
					now.Add(-10*time.Hour),
					now.Add(-5*time.Hour),
				)
				require.NoError(t, err)
			},
			after: func() {
				_, err := postgresConn.Exec(ctx, `DELETE FROM transactions_normalized`)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before()
			txsRepo := NewTxs(postgresConn)
			count, err := txsRepo.GetWalletsCountPerPeriod(ctx, tt.params.startDate, tt.params.endDate)
			require.Equal(t, tt.expected.err, err)
			require.Equal(t, tt.expected.count, count)
			tt.after()
		})
	}
}

func TestTxs_DelegatesByValidator(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from txes`)
		require.NoError(t, err)
		_, err = postgresConn.Exec(context.Background(), `delete from tx_delegate_aggregateds`)
		require.NoError(t, err)
	}()

	txes := `INSERT INTO txes (id, hash, code, block_id, signatures, timestamp, memo, timeout_height, extension_options, non_critical_extension_options, auth_info_id, tx_response_id)
									VALUES
									  (1, 'hash1', 123, 1, '{"signature1", "signature2"}', $1, 'Random memo 1', 100, '{"option1", "option2"}', '{"non_critical_option1", "non_critical_option2"}', 1, 1),
									  (2, 'hash2', 456, 2, '{"signature3", "signature4"}', $1, 'Random memo 2', 200, '{"option3", "option4"}', '{"non_critical_option3", "non_critical_option4"}', 2, 2),
									  (3, 'hash3', 789, 3, '{"signature5", "signature6"}', $1, 'Random memo 3', 300, '{"option5", "option6"}', '{"non_critical_option5", "non_critical_option6"}', 3, 3),
									  (4, 'hash4', 101112, 4, '{"signature7", "signature8"}', $1, 'Random memo 4', 400, '{"option7", "option8"}', '{"non_critical_option7", "non_critical_option8"}', 4, 4)
									  `
	_, err := postgresConn.Exec(context.Background(), txes, time.Now().UTC())
	require.NoError(t, err)

	txDelegates := `INSERT INTO tx_delegate_aggregateds(hash, tx_type, timestamp, validator, block_height, amount, denom, sender) 
					VALUES 
					('hash1', 'delegate', $1, 'valoper1', 1, 100000, 'utia', 'sender1'),
					('hash2', 'delegate', $1, 'valoper1', 1, 600, 'utia', 'sender2'),
					('hash3', 'delegate', $1, 'valoper2', 1, 700, 'utia', 'sender2'),
					('hash4', 'delegate', $2, 'valoper1', 1, 700, 'utia', 'sender2')`
	_, err = postgresConn.Exec(context.Background(), txDelegates, time.Now().UTC(), time.Now().UTC().Add(-48*time.Hour))
	require.NoError(t, err)

	txsRepo := NewTxs(postgresConn)
	txsRes, sum, all, err := txsRepo.DelegatesByValidator(context.Background(), time.Now().Add(-5*time.Hour), time.Now(),
		"valoper1", 1, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(2))
	require.Len(t, txsRes, 1)
	require.Equal(t, sum.Amount, "100600")
	require.Equal(t, sum.Denom, "utia")
}

func Test_GetVotesByAccounts(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from votes_normalized`)
		require.NoError(t, err)
	}()

	votes := `INSERT INTO votes_normalized(hash, weight, proposal_id, height, timestamp, option, voter)
				VALUES('hash1', '1000', '2', 900, $1, 'YES', 'voter1'),
				      ('hash2', '2000', '2', 900, $1, 'NO', 'voter2'),
				      ('hash21', '2000', '2', 900, $1, 'YES', 'voter7'),
				      ('hash3', '2000', '2', 900, $1, 'ABSTAIN', 'voter3'),
				      ('hash4', '2000', '2', 900, $1, 'NO_VETO', 'voter4'),
				      ('hash5', '2000', '3', 900, $1, 'YES', 'voter4'),
				      ('hash51', '2000', '3', 900, $1, 'YES', 'voter7')`
	_, err := postgresConn.Exec(context.Background(), votes, time.Now().UTC())
	require.NoError(t, err)

	txsRepo := NewTxs(postgresConn)
	res, all, err := txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1"}, false,
		"YES", 2, nil, 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	res, all, err = txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1", "voter7"}, false,
		"YES", 2, nil, 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, all, int64(2))
	require.Len(t, res, 2)

	res, all, err = txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1"}, true,
		"YES", 2, nil, 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	filterBy := "voter7"
	res, all, err = txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1", "voter7"}, true,
		"YES", 2, &filterBy, 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, all, int64(0))
	require.Len(t, res, 0)

	res, all, err = txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1", "voter4"}, true,
		"YES", 3, &filterBy, 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	res, all, err = txsRepo.GetVotesByAccounts(context.Background(),
		[]string{"voter1", "voter4"}, true,
		"YES", 3, &filterBy, 100, 0,
		&model.SortBy{By: "timestamp", Direction: "desc"})
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)
}

func Test_GetVotes(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from votes_normalized`)
		require.NoError(t, err)
	}()

	votes := `INSERT INTO votes_normalized(hash, weight, proposal_id, height, timestamp, option, voter)
		VALUES('hash1', '1000', '2', 900, $1, 'YES', 'voter1'),
			  ('hash2', '500', '3', 901, $1, 'NO', 'voter1'),
			  ('hash3', '200', '4', 902, $1, 'YES', 'voter2'),
			  ('hash4', '200', '5', 902, $1, 'YES', 'voter5'),
			  ('hash5', '200', '6', 902, $1, 'YES', 'voter5'),
			  ('hash6', '200', '6', 902, $1, 'YES', 'voter5'),
			  ('hash7', '200', '7', 902, $1, 'YES', 'voter5'),
			  ('hash8', '200', '7', 902, $1, 'YES', 'voter5');`
	_, err := postgresConn.Exec(context.Background(), votes, time.Now().UTC())
	require.NoError(t, err)

	txsRepo := NewTxs(postgresConn)

	t.Run("returns votes for a given voter", func(t *testing.T) {
		res, total, err := txsRepo.GetVotes(context.Background(), "voter1", false, 100, 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), total)
		require.Len(t, res, 2)
		require.Equal(t, "hash1", res[0].TxHash)
		require.Equal(t, "hash2", res[1].TxHash)
	})

	t.Run("returns empty result for a voter with no votes", func(t *testing.T) {
		res, total, err := txsRepo.GetVotes(context.Background(), "voter3", false, 100, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), total)
		require.Len(t, res, 0)
	})

	t.Run("returns votes with correct proposal ID", func(t *testing.T) {
		res, total, err := txsRepo.GetVotes(context.Background(), "voter2", false, 100, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), total)
		require.Len(t, res, 1)
		require.Equal(t, 4, res[0].ProposalID)
	})

	t.Run("handles invalid proposal ID", func(t *testing.T) {
		invalidVotes := `INSERT INTO votes_normalized(hash, weight, proposal_id, height, timestamp, option, voter)
			VALUES('hash4', '300', 'invalid', 903, $1, 'YES', 'voter4');`
		_, err := postgresConn.Exec(context.Background(), invalidVotes, time.Now().UTC())
		require.NoError(t, err)

		_, _, err = txsRepo.GetVotes(context.Background(), "voter4", false, 100, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid proposal ID")
	})

	t.Run("returns unique votes on proposals", func(t *testing.T) {
		res, total, err := txsRepo.GetVotes(context.Background(), "voter5", true, 100, 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), total)
		require.Len(t, res, 3)
		require.Equal(t, 5, res[0].ProposalID)
		require.Equal(t, 6, res[1].ProposalID)
		require.Equal(t, 7, res[2].ProposalID)
	})
}

func Test_GetProposalDeposits(t *testing.T) {
	defer func() {
		_, err := postgresConn.Exec(context.Background(), `delete from depositors_normalized`)
		require.NoError(t, err)
	}()

	query := `INSERT INTO depositors_normalized(id, hash, timestamp, proposal_id, height, sender, amount, denom)
				VALUES(1, 'hash-11', now(), 2, 1, 'sender-1', 10000, 'tia'),
				      (1, 'hash-1122', now(), 3, 1, 'sender-1', 1000000, 'tia')`
	_, err := postgresConn.Exec(context.Background(), query)
	require.NoError(t, err)

	txsRepo := NewTxs(postgresConn)
	res, all, err := txsRepo.ProposalDepositors(context.Background(),
		2, nil, 100, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	res, all, err = txsRepo.ProposalDepositors(context.Background(),
		3, nil, 100, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	res, all, err = txsRepo.ProposalDepositors(context.Background(),
		7, nil, 100, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(0))
	require.Len(t, res, 0)

	res, all, err = txsRepo.ProposalDepositors(context.Background(),
		2, &model.SortBy{By: "timestamp", Direction: "desc"}, 100, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)

	res, all, err = txsRepo.ProposalDepositors(context.Background(),
		2, &model.SortBy{By: "amount", Direction: "asc"}, 100, 0)
	require.NoError(t, err)
	require.Equal(t, all, int64(1))
	require.Len(t, res, 1)
}
