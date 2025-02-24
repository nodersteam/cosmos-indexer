package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
	"github.com/rs/zerolog/log"
	migrate "github.com/xakep666/mongo-migrate"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LocalMigrator interface {
	Migrate(ctx context.Context) (*mongo.Database, error)

	txEventsMigration(ctx context.Context) error
}

type localMigrator struct {
	db     *mongo.Database
	pg     *pgxpool.Pool
	search repository.Search
	txs    repository.Txs
}

func NewLocalMigrator(db *mongo.Database, pg *pgxpool.Pool, search repository.Search, txs repository.Txs) LocalMigrator {
	return &localMigrator{db: db, pg: pg, search: search, txs: txs}
}

func (j *localMigrator) Migrate(ctx context.Context) (*mongo.Database, error) {
	m := migrate.NewMigrate(j.db, migrate.Migration{
		Version:     1,
		Description: "add unique index idx_txhash_type",
		Up: func(ctx context.Context, db *mongo.Database) error {
			log.Info().Msgf("starting v1 migration")

			err := db.Collection("search").Drop(ctx)
			if err != nil {
				return err
			}

			opt := options.Index().SetName("idx_txhash_type").SetUnique(true)
			keys := bson.D{{"tx_hash", 1}, {"type", 1}} //nolint
			mdl := mongo.IndexModel{Keys: keys, Options: opt}
			_, err = db.Collection("search").Indexes().CreateOne(ctx, mdl)
			if err != nil {
				log.Err(err).Msgf("error creating index for v1 migration")
				return err
			}

			log.Info().Msgf("v1 migration completed")
			return nil
		},
		Down: func(ctx context.Context, db *mongo.Database) error {
			_, err := db.Collection("search").Indexes().DropOne(ctx, "idx_txhash_type")
			if err != nil {
				log.Err(err).Msgf("error dropping index for v1 migration")
				return err
			}
			return nil
		},
	}, migrate.Migration{ // TODO not the best place to migrate data
		Version:     2,
		Description: "migrate existing hashes",
		Up: func(ctx context.Context, db *mongo.Database) error {
			log.Info().Msgf("starting txs v2 migration")
			rows, err := j.pg.Query(ctx, `select distinct hash from txes`)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			for rows.Next() {
				var txHash string
				if err = rows.Scan(&txHash); err != nil {
					return err
				}
				if err = j.search.AddHash(context.Background(), txHash, "transaction", 0); err != nil {
					log.Err(err).Msgf("Failed to add hash to index transaction %s", txHash)
				}
			}

			log.Info().Msgf("v2 blocks migration")
			rows, err = j.pg.Query(ctx, `select distinct block_hash from blocks`)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			for rows.Next() {
				var txHash string
				if err = rows.Scan(&txHash); err != nil {
					return err
				}
				if err = j.search.AddHash(context.Background(), txHash, "block", 0); err != nil {
					log.Err(err).Msgf("Failed to add hash to index block %s", txHash)
				}
			}

			log.Info().Msgf("v2 migration completed")
			return nil
		},
		Down: func(ctx context.Context, db *mongo.Database) error {
			// ignoring, what's done is done.
			return nil
		},
	}, migrate.Migration{ // TODO not the best place to migrate data
		Version:     3,
		Description: "migrate existing hashes with block height",
		Up: func(ctx context.Context, db *mongo.Database) error {
			log.Info().Msgf("starting txs v3 migration")
			err := db.Collection("search").Drop(ctx)
			if err != nil {
				log.Err(err).Msgf("Failed to drop index, continue")
			}

			rows, err := j.pg.Query(ctx, `select distinct hash from txes`)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			for rows.Next() {
				var txHash string
				if err = rows.Scan(&txHash); err != nil {
					return err
				}
				if err = j.search.AddHash(context.Background(), txHash, "transaction", 0); err != nil {
					log.Err(err).Msgf("Failed to add hash to index")
				}
			}

			log.Info().Msgf("starting blocks migration")
			rows, err = j.pg.Query(ctx, `select distinct block_hash,height from blocks`)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			for rows.Next() {
				var txHash string
				var blockHeight int64
				if err = rows.Scan(&txHash, &blockHeight); err != nil {
					return err
				}
				if err = j.search.AddHash(context.Background(), txHash, "block", blockHeight); err != nil {
					log.Err(err).Msgf("Failed to add hash to index")
				}
			}

			log.Info().Msgf("v3 migration completed")
			return nil
		},
		Down: func(ctx context.Context, db *mongo.Database) error {
			// ignoring, what's done is done.
			return nil
		},
	}, migrate.Migration{
		Version:     4,
		Description: "migrate tx_events to tx_events_vals_aggregated",
		Up: func(ctx context.Context, db *mongo.Database) error {
			log.Info().Msgf("v4 starting tx_events_vals_migration")
			return j.txEventsValsMigration(ctx)
		},
		Down: func(ctx context.Context, db *mongo.Database) error {
			return nil
		},
	}, migrate.Migration{
		Version:     5,
		Description: "migrate tx_events to tx_events_aggregated",
		Up: func(ctx context.Context, db *mongo.Database) error {
			log.Info().Msgf("v5 starting tx_events_migration")
			return j.txEventsMigration(ctx)
		},
		Down: func(ctx context.Context, db *mongo.Database) error {
			return nil
		},
	})
	if err := m.Up(ctx, migrate.AllAvailable); err != nil {
		return nil, err
	}

	return j.db, nil
}

func (j *localMigrator) txEventsValsMigration(ctx context.Context) error {
	_, err := j.pg.Exec(ctx, `INSERT INTO tx_events_vals_aggregateds (ev_attr_value, msg_type, tx_hash, tx_timestamp) (select
                                                   MD5(LOWER(message_event_attributes.value)),
                                                  message_types.message_type,
                                                   txes.hash,
                                                   txes.timestamp
     from txes
         left join messages on txes.id = messages.tx_id
         left join message_types on messages.message_type_id = message_types.id
         left join message_events on messages.id = message_events.message_id
         left join message_event_types on message_events.message_event_type_id=message_event_types.id
         left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
         left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
order by messages.message_index, message_events.index, message_event_attributes.index) ON CONFLICT DO NOTHING;`)
	return err
}

func (j *localMigrator) txEventsMigration(ctx context.Context) error {
	_, err := j.pg.Exec(ctx, `INSERT INTO tx_events_aggregateds(tx_hash, message_type, message_type_index, message_event_type, message_event_attr_index, message_event_attr_value, message_event_attr_key)
(select
     txes.hash,
    message_types.message_type,
    COALESCE(message_events.index, 0),
    message_event_types.type,
    COALESCE(message_event_attributes.index, 0),
    message_event_attributes.value,
    message_event_attribute_keys.key
from txes
         left join messages on txes.id = messages.tx_id
         left join message_types on messages.message_type_id = message_types.id
         left join message_events on messages.id = message_events.message_id
         left join message_event_types on message_events.message_event_type_id=message_event_types.id
         left join message_event_attributes on message_events.id = message_event_attributes.message_event_id
         left join message_event_attribute_keys on message_event_attributes.message_event_attribute_key_id = message_event_attribute_keys.id
order by messages.message_index, message_events.index, message_event_attributes.index) ON CONFLICT DO NOTHING;`)
	return err
}
