package repository

import (
	"context"
	"fmt"
	"regexp"

	"github.com/rs/zerolog/log"

	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/nodersteam/cosmos-indexer/pkg/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const searchCollection = "search"

type Search interface {
	AddHash(ctx context.Context, hash string, hashType string, blockHeight int64) error
	HashByText(ctx context.Context, text string) ([]model.SearchResult, error)
	BlockByHeight(ctx context.Context, blockHeight int64) ([]model.SearchResult, error)
}

type search struct {
	pool *mongo.Database
}

func NewSearch(pool *mongo.Database) Search {
	return &search{pool: pool}
}

func (a *search) AddHash(ctx context.Context, hash string, hashType string, blockHeight int64) error {
	searchResult := model.SearchResult{TxHash: hash, Type: hashType, BlockHeight: fmt.Sprintf("%d", blockHeight)}

	filter := bson.D{primitive.E{Key: "tx_hash", Value: hash}, primitive.E{Key: "type", Value: hashType}}
	update := bson.D{{ //nolint: govet
		"$set",
		searchResult,
	}}

	upsert := true
	opts := options.UpdateOptions{Upsert: &upsert}

	res, err := a.pool.Collection(searchCollection).UpdateOne(ctx,
		filter, update, &opts)
	if err != nil {
		return err
	}
	log.Debug().Msgf("upserted count %d %s %s %d", res.UpsertedCount, hash, hashType, blockHeight)
	return nil
}

func (a *search) HashByText(ctx context.Context, text string) ([]model.SearchResult, error) {
	filter := bson.D{{"tx_hash", primitive.Regex{Pattern: regexp.QuoteMeta(text), Options: "i"}}} //nolint: govet
	cursor, err := a.pool.Collection(searchCollection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var dbResult []model.SearchResult
	if err = cursor.All(ctx, &dbResult); err != nil {
		return nil, err
	}
	return dbResult, nil
}

func (a *search) BlockByHeight(ctx context.Context, blockHeight int64) ([]model.SearchResult, error) {
	filter := bson.D{
		{"block_height", primitive.Regex{Pattern: fmt.Sprintf("%d", blockHeight), Options: "i"}}, //nolint: govet
	}
	cursor, err := a.pool.Collection(searchCollection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var dbResult []model.SearchResult
	if err = cursor.All(ctx, &dbResult); err != nil {
		return nil, err
	}
	return dbResult, nil
}
