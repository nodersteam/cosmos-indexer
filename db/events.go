package db

import (
	"fmt"
	"github.com/nodersteam/cosmos-indexer/config"
	"github.com/nodersteam/cosmos-indexer/db/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"unicode/utf8"
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
		if err := dbTransaction.Where(&consAddress).FirstOrCreate(&consAddress).Error; err != nil {
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
			Where(models.Block{Height: blockDBWrapper.Block.Height,
				ChainID: blockDBWrapper.Block.ChainID}).
			Assign(models.Block{BlockEventsIndexed: true,
				TimeStamp:           blockDBWrapper.Block.TimeStamp,
				ProposerConsAddress: blockDBWrapper.Block.ProposerConsAddress})
		err := tx.FirstOrCreate(&blockDBWrapper.Block).Error
		if err != nil {
			config.Log.Error("Error getting/creating block DB object.", err)
			return err
		}

		// saving signatures
		if len(signaturesCopy) > 0 {
			for ind, _ := range signaturesCopy {
				signaturesCopy[ind].BlockID = uint64(blockDBWrapper.Block.ID)
			}

			err = dbTransaction.Clauses(
				clause.OnConflict{
					Columns:   []clause.Column{{Name: "block_id"}, {Name: "validator_address"}},
					UpdateAll: true,
				}).CreateInBatches(signaturesCopy, 1000).Error
			if err != nil {
				config.Log.Error("Error creating block signatures.", err)
				return err
			}
			blockDBWrapper.Block.Signatures = signaturesCopy
		}

		var uniqueBlockEventTypes []models.BlockEventType

		for _, value := range blockDBWrapper.UniqueBlockEventTypes {
			uniqueBlockEventTypes = append(uniqueBlockEventTypes, value)
		}

		// Bulk find or create on unique event types
		if err := dbTransaction.Clauses(
			clause.Returning{
				Columns: []clause.Column{
					{Name: "id"}, {Name: "type"},
				},
			},
			clause.OnConflict{
				Columns:   []clause.Column{{Name: "type"}},
				DoUpdates: clause.AssignmentColumns([]string{"type"}),
			},
		).CreateInBatches(&uniqueBlockEventTypes, 1000).Error; err != nil {
			config.Log.Error("Error creating begin block event types.", err)
			return err
		}

		for _, value := range uniqueBlockEventTypes {
			blockDBWrapper.UniqueBlockEventTypes[value.Type] = value
		}

		var uniqueBlockEventAttributeKeys []models.BlockEventAttributeKey

		for _, value := range blockDBWrapper.UniqueBlockEventAttributeKeys {
			uniqueBlockEventAttributeKeys = append(uniqueBlockEventAttributeKeys, value)
		}

		if err := dbTransaction.Clauses(
			clause.Returning{
				Columns: []clause.Column{
					{Name: "id"}, {Name: "key"},
				},
			},
			clause.OnConflict{
				Columns:   []clause.Column{{Name: "key"}},
				DoUpdates: clause.AssignmentColumns([]string{"key"}),
			},
		).CreateInBatches(&uniqueBlockEventAttributeKeys, 1000).Error; err != nil {
			config.Log.Error("Error creating begin block event attribute keys.", err)
			return err
		}

		for _, value := range uniqueBlockEventAttributeKeys {
			blockDBWrapper.UniqueBlockEventAttributeKeys[value.Key] = value
		}

		// Loop through begin and end block arrays and apply the block ID and event type ID
		beginBlockEvents := make([]*models.BlockEvent, len(blockDBWrapper.BeginBlockEvents))
		for index := range blockDBWrapper.BeginBlockEvents {
			blockDBWrapper.BeginBlockEvents[index].BlockEvent.Block = *blockDBWrapper.Block
			blockDBWrapper.BeginBlockEvents[index].BlockEvent.BlockID = blockDBWrapper.Block.ID
			blockDBWrapper.BeginBlockEvents[index].BlockEvent.BlockEventType = blockDBWrapper.UniqueBlockEventTypes[blockDBWrapper.BeginBlockEvents[index].BlockEvent.BlockEventType.Type]
			beginBlockEvents[index] = &blockDBWrapper.BeginBlockEvents[index].BlockEvent
		}

		endBlockEvents := make([]*models.BlockEvent, len(blockDBWrapper.EndBlockEvents))
		for index := range blockDBWrapper.EndBlockEvents {
			blockDBWrapper.EndBlockEvents[index].BlockEvent.Block = *blockDBWrapper.Block
			blockDBWrapper.EndBlockEvents[index].BlockEvent.BlockID = blockDBWrapper.Block.ID
			blockDBWrapper.EndBlockEvents[index].BlockEvent.BlockEventType = blockDBWrapper.UniqueBlockEventTypes[blockDBWrapper.EndBlockEvents[index].BlockEvent.BlockEventType.Type]
			endBlockEvents[index] = &blockDBWrapper.EndBlockEvents[index].BlockEvent
		}

		// Bulk insert the block events
		allBlockEvents := make([]*models.BlockEvent, len(beginBlockEvents)+len(endBlockEvents))
		copy(allBlockEvents, beginBlockEvents)
		copy(allBlockEvents[len(beginBlockEvents):], endBlockEvents)

		// TODO: Should consider the on conflict values here, do we want to provide the user with some control over the behavior here?
		// Something similar to our reindex flag may be appropriate, unless we just want to have that pre-check the block has already been indexed.
		if len(allBlockEvents) != 0 {
			// This clause forces a return of ID for all items even on conflict
			// We need this so that we can then create the proper associations with the attributes below
			if err := dbTransaction.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{{Name: "index"}, {Name: "lifecycle_position"}, {Name: "block_id"}},
					// Force update of block event type ID
					DoUpdates: clause.AssignmentColumns([]string{"block_event_type_id"}),
				},
			).CreateInBatches(&allBlockEvents, 1000).Error; err != nil {
				config.Log.Error("Error creating begin block events.", err)
				return err
			}
			var allAttributes []*models.BlockEventAttribute
			for index := range blockDBWrapper.BeginBlockEvents {
				currAttributes := blockDBWrapper.BeginBlockEvents[index].Attributes
				for attrIndex := range currAttributes {
					currAttributes[attrIndex].BlockEventID = blockDBWrapper.BeginBlockEvents[index].BlockEvent.ID
					currAttributes[attrIndex].BlockEvent = blockDBWrapper.BeginBlockEvents[index].BlockEvent
					if !utf8.ValidString(currAttributes[attrIndex].Value) || strings.Contains(currAttributes[attrIndex].Value, "\x00") {
						currAttributes[attrIndex].Value = "-"
					}
					currAttributes[attrIndex].BlockEventAttributeKey = blockDBWrapper.UniqueBlockEventAttributeKeys[currAttributes[attrIndex].BlockEventAttributeKey.Key]
				}
				for ii := range currAttributes {
					allAttributes = append(allAttributes, &currAttributes[ii])
				}
			}

			for index := range blockDBWrapper.EndBlockEvents {
				currAttributes := blockDBWrapper.EndBlockEvents[index].Attributes
				for attrIndex := range currAttributes {
					currAttributes[attrIndex].BlockEventID = blockDBWrapper.EndBlockEvents[index].BlockEvent.ID
					currAttributes[attrIndex].BlockEvent = blockDBWrapper.EndBlockEvents[index].BlockEvent
					if !utf8.ValidString(currAttributes[attrIndex].Value) || strings.Contains(currAttributes[attrIndex].Value, "\x00") {
						currAttributes[attrIndex].Value = "-"
					}
					currAttributes[attrIndex].BlockEventAttributeKey =
						blockDBWrapper.UniqueBlockEventAttributeKeys[currAttributes[attrIndex].BlockEventAttributeKey.Key]
				}
				for ii := range currAttributes {
					allAttributes = append(allAttributes, &currAttributes[ii])
				}
			}

			if len(allAttributes) != 0 {
				if err := dbTransaction.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "block_event_id"}, {Name: "index"}},
					// Force update of value
					DoUpdates: clause.AssignmentColumns([]string{"value"}),
				}).CreateInBatches(&allAttributes, 1000).Error; err != nil {
					config.Log.Error("Error creating begin block event attributes. continue", err)
					return err
				}
			}

		}

		return nil
	})

	// Contract: ensure that wrapper has been loaded with all data before returning
	return blockDBWrapper, err
}

func IndexCustomBlockEvents(conf config.IndexConfig, db *gorm.DB, dryRun bool, blockDBWrapper *BlockDBWrapper, identifierLoggingString string, beginBlockParserTrackers map[string]models.BlockEventParser, endBlockParserTrackers map[string]models.BlockEventParser) error {
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
