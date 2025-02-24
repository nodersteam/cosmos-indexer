package db

import (
	"errors"
	"fmt"

	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/db/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func IndexBlockEvents(db *gorm.DB, blockDBWrapper *BlockDBWrapper) (*BlockDBWrapper, error) {
	err := db.Transaction(func(dbTransaction *gorm.DB) error {
		if err := dbTransaction.
			Exec("DELETE FROM failed_event_blocks WHERE height = ? AND blockchain_id = ?", blockDBWrapper.Block.Height, blockDBWrapper.Block.ChainID).
			Error; err != nil {
			config.Log.Error("Error updating failed block.", err)
			return err
		}

		consAddress := blockDBWrapper.Block.ProposerConsAddress

		// create cons address if it doesn't exist
		if err := dbTransaction.Where(&consAddress).Clauses(
			clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}},
				DoUpdates: clause.AssignmentColumns([]string{"address"}),
			}).
			FirstOrCreate(&consAddress).Error; err != nil {
			config.Log.Error("Error getting/creating cons address DB object.", err)
			return err
		}

		// create block if it doesn't exist
		blockDBWrapper.Block.ProposerConsAddressID = consAddress.ID
		blockDBWrapper.Block.ProposerConsAddress = consAddress

		// create block if it doesn't exist
		blockDBWrapper.Block.BlockEventsIndexed = true

		if len(blockDBWrapper.Block.Signatures) == 0 {
			config.Log.Warn(fmt.Sprintf("No signatures found in block. %d", blockDBWrapper.Block.Height))
		}

		signaturesCopy := make([]models.BlockSignature, len(blockDBWrapper.Block.Signatures))
		copy(signaturesCopy, blockDBWrapper.Block.Signatures)
		blockDBWrapper.Block.Signatures = make([]models.BlockSignature, 0)

		tx := dbTransaction.
			Where(models.Block{
				Height:  blockDBWrapper.Block.Height,
				ChainID: blockDBWrapper.Block.ChainID,
			}).
			Assign(models.Block{
				ID:                  blockDBWrapper.Block.ID,
				BlockEventsIndexed:  true,
				TimeStamp:           blockDBWrapper.Block.TimeStamp,
				ProposerConsAddress: blockDBWrapper.Block.ProposerConsAddress,
			}).
			Clauses(
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				clause.OnConflict{
					Columns:   []clause.Column{{Name: "height"}, {Name: "chain_id"}},
					DoNothing: true,
				})
		err := tx.FirstOrCreate(&blockDBWrapper.Block).Error
		if err != nil {
			config.Log.Error("Error getting/creating block DB object", err)
			return err
		}

		// Ensure block ID is set before saving signatures
		blockID := blockDBWrapper.Block.ID
		if blockID == 0 {
			config.Log.Errorf("Block ID is not set. Cannot save signatures. %d", blockDBWrapper.Block.Height)
			return errors.New("block ID is not set")
		}

		// saving signatures
		if len(signaturesCopy) > 0 {
			for ind := range signaturesCopy {
				signaturesCopy[ind].BlockID = uint64(blockID)
			}

			err = dbTransaction.Clauses(
				clause.OnConflict{
					Columns:   []clause.Column{{Name: "block_id"}, {Name: "validator_address"}},
					DoNothing: true,
				}).CreateInBatches(signaturesCopy, 1000).Error
			if err != nil {
				config.Log.Error("Error creating block signatures.", err)
				return err
			}
			blockDBWrapper.Block.Signatures = signaturesCopy
		}

		return nil
	})

	// Contract: ensure that wrapper has been loaded with all data before returning
	return blockDBWrapper, err
}

func IndexCustomBlockEvents(conf config.IndexConfig, db *gorm.DB, blockDBWrapper *BlockDBWrapper, identifierLoggingString string, beginBlockParserTrackers map[string]models.BlockEventParser, endBlockParserTrackers map[string]models.BlockEventParser) error {
	return db.Transaction(func(dbTransaction *gorm.DB) error {
		for _, beginBlockEvents := range blockDBWrapper.BeginBlockEvents {
			if len(beginBlockEvents.BlockEventParsedDatasets) != 0 {
				for _, parsedData := range beginBlockEvents.BlockEventParsedDatasets {
					if parsedData.Error == nil && parsedData.Data != nil && parsedData.Parser != nil {
						err := (*parsedData.Parser).IndexBlockEvent(parsedData.Data, dbTransaction, *blockDBWrapper.Block, beginBlockEvents.BlockEvent, beginBlockEvents.Attributes, conf)
						if err != nil {
							config.Log.Error("Error indexing block event.", err)
							return err
						}
					} else if parsedData.Error != nil {
						err := CreateParserError(db, beginBlockEvents.BlockEvent, beginBlockParserTrackers[(*parsedData.Parser).Identifier()], parsedData.Error)
						if err != nil {
							config.Log.Error("Error indexing block event error.", err)
							return err
						}
					}
				}
			}
		}

		for _, endBlockEvents := range blockDBWrapper.EndBlockEvents {
			if len(endBlockEvents.BlockEventParsedDatasets) != 0 {
				for _, parsedData := range endBlockEvents.BlockEventParsedDatasets {
					if parsedData.Error == nil && parsedData.Data != nil && parsedData.Parser != nil {
						err := (*parsedData.Parser).IndexBlockEvent(parsedData.Data, dbTransaction, *blockDBWrapper.Block, endBlockEvents.BlockEvent, endBlockEvents.Attributes, conf)
						if err != nil {
							config.Log.Error("Error indexing block event.", err)
						}
					} else if parsedData.Error != nil {
						err := CreateParserError(db, endBlockEvents.BlockEvent, endBlockParserTrackers[(*parsedData.Parser).Identifier()], parsedData.Error)
						if err != nil {
							config.Log.Error("Error indexing block event error.", err)
							return err
						}
					}
				}
			}
		}

		return nil
	})
}
