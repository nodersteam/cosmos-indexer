package db

import (
	"github.com/noders-team/cosmos-indexer/db/models"
	"gorm.io/gorm"
)

func FindOrCreateCustomParsers(db *gorm.DB, parsers map[string]models.BlockEventParser) error {
	err := db.Transaction(func(dbTransaction *gorm.DB) error {
		for key := range parsers {
			currParser := parsers[key]
			res := dbTransaction.FirstOrCreate(&currParser, &currParser)

			if res.Error != nil {
				return res.Error
			}
			parsers[key] = currParser
		}
		return nil
	})
	return err
}

func CreateParserError(db *gorm.DB, blockEvent models.BlockEvent, parser models.BlockEventParser, parserError error) error {
	err := db.Transaction(func(dbTransaction *gorm.DB) error {
		parserErrorRecord := &models.BlockEventParserError{
			BlockEventParser: parser,
			BlockEvent:       blockEvent,
			Error:            parserError.Error(),
		}
		res := dbTransaction.FirstOrCreate(parserErrorRecord)
		return res.Error
	})
	return err
}
