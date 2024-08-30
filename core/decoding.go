package core

import (
	txv1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	"github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx"
	probeClient "github.com/nodersteam/probe/client"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// InAppTxDecoder Provides an in-app tx decoder.
// The primary use-case for this function is to allow fallback decoding if a TX fails to decode after RPC requests.
// This can happen in a number of scenarios, but mainly due to missing proto definitions.
// We can attempt a personal decode of the TX, and see if we can continue indexing based on in-app conditions (such as message type filters).
// This function skips a large chunk of decoding validations, and is not recommended for general use. Its main point is to skip errors that in
// default Cosmos TX decoders would cause the entire decode to fail.
func InAppTxDecoder(cdc probeClient.Codec) sdk.TxDecoder {
	return func(txBytes []byte) (sdk.Tx, error) {
		var raw tx.TxRaw
		var err error

		err = cdc.Marshaler.Unmarshal(txBytes, &raw)
		if err != nil {
			return nil, err
		}

		body := tx.TxBody{}
		var bodyV1 txv1beta1.TxBody
		err = proto.Unmarshal(raw.BodyBytes, &bodyV1)
		if err != nil {
			log.Err(err).Msgf("failed to unmarshal tx body bytes")
			return nil, err
		}
		for _, mm := range bodyV1.Messages {
			body.Messages = append(body.Messages, &types.Any{TypeUrl: mm.TypeUrl, Value: mm.Value})
		}
		body.Memo = bodyV1.Memo
		body.TimeoutHeight = bodyV1.TimeoutHeight
		// TODO extension options

		var authInfo tx.AuthInfo
		err = cdc.Marshaler.Unmarshal(raw.AuthInfoBytes, &authInfo)
		if err != nil {
			log.Err(err).Msgf("failed to unmarshal auth info, transaction will be ignored")
		}

		// TODO might be required, keep for now
		/*
			var authInfoV1 txv1beta1.AuthInfo
			err = proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw.AuthInfoBytes, &authInfoV1)
			if err != nil {
				log.Err(err).Msgf("failed to unmarshal auth info, transaction will be ignored")
				//return nil, errors.New("failed to unmarshal auth info " + err.Error())
			}

			authInfo := tx.AuthInfo{}
			if authInfoV1.Fee != nil {
				authInfo.Fee = &tx.Fee{
					Amount:   fromV1Amount(authInfoV1.Fee.Amount),
					GasLimit: authInfoV1.Fee.GasLimit,
					Payer:    authInfoV1.Fee.Payer,
					Granter:  authInfoV1.Fee.Granter,
				}
			}
			if len(authInfoV1.SignerInfos) > 0 {
				authInfo.SignerInfos = fromV1SignerInfos(authInfoV1.SignerInfos)
			}*/

		theTx := &tx.Tx{
			Body:       &body,
			AuthInfo:   &authInfo,
			Signatures: raw.Signatures,
		}

		return theTx, nil
	}
}
