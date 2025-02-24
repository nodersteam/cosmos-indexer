package repository

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
)

var postgresConn *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		log.Err(err).Msgf("Could not connect to docker: %s", err)
	}

	resourcePostgres := initializePostgres(ctx, dockerPool, newPostgresConfig())

	postgresManualMigration(ctx)

	// Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	purgeResources(dockerPool, resourcePostgres)

	cancel()
	os.Exit(code)
}

func initializePostgres(ctx context.Context, dockerPool *dockertest.Pool, cfg *postgresConfig) *dockertest.Resource {
	resource, err := dockerPool.Run(cfg.Repository, cfg.Version, cfg.EnvVariables)
	if err != nil {
		log.Err(err).Msgf("Could not start resource: %s", err)
	}

	var dbHostAndPort string

	err = dockerPool.Retry(func() error {
		var dbHost string

		gitlabCIHost := os.Getenv("DATABASE_HOST")

		if gitlabCIHost != "" {
			dbHost = gitlabCIHost
		} else {
			dbHost = "localhost"
		}

		port := resource.GetPort(cfg.PortID)
		dbHostAndPort = fmt.Sprintf("%s:%s", dbHost, port)

		dsn := cfg.getConnectionString(dbHostAndPort)

		postgresConn, err = pgxpool.New(ctx, dsn)
		if err != nil {
			return fmt.Errorf("connect: %v", err)
		}

		if err = postgresConn.Ping(ctx); err != nil {
			return fmt.Errorf("ping: %v", err)
		}

		return nil
	})
	if err != nil {
		log.Err(err).Msgf("Could not connect to database: %s", err)
	}
	log.Info().Msgf(strings.Join(cfg.getFlywayMigrationArgs(dbHostAndPort), " "))
	cmd := exec.Command("/usr/local/bin/flyway", cfg.getFlywayMigrationArgs(dbHostAndPort)...) //nolint:gosec

	err = cmd.Run()
	if err != nil {
		log.Err(err).Msgf("There are errors in migrations: %v", err)
	}
	return resource
}

type postgresConfig struct {
	Repository   string
	Version      string
	EnvVariables []string
	PortID       string
	DB           string
}

func newPostgresConfig() *postgresConfig {
	return &postgresConfig{
		Repository: "postgres",
		Version:    "14.1-alpine",
		EnvVariables: []string{
			"POSTGRES_PASSWORD=password123",
			"POSTGRES_DB=db",
			"listen_addresses = '*'",
		},
		PortID: "5432/tcp",
		DB:     "db",
	}
}

func (p *postgresConfig) getConnectionString(dbHostAndPort string) string {
	return fmt.Sprintf("postgresql://postgres:password123@%v/%s?sslmode=disable", dbHostAndPort, p.DB)
}

func (p *postgresConfig) getFlywayMigrationArgs(dbHostAndPort string) []string {
	return []string{
		"-user=postgres",
		"-password=password123",
		"-locations=filesystem:../../migrations",
		fmt.Sprintf("-url=jdbc:postgresql://%v/%s", dbHostAndPort, p.DB),
		"migrate",
	}
}

func purgeResources(dockerPool *dockertest.Pool, resources ...*dockertest.Resource) {
	for i := range resources {
		if err := dockerPool.Purge(resources[i]); err != nil {
			log.Err(err).Msgf("Could not purge resource: %s", err)
		}
		err := resources[i].Expire(1)
		if err != nil {
			log.Err(err).Msgf("%s", err)
		}
	}
}

func postgresManualMigration(ctx context.Context) {
	// TODO add this into migration files
	migrations := make([]string, 0)

	queryTxes := `
	create table txes
	(
		id                             bigserial primary key,
		hash                           text,
		code                           bigint,
		block_id                       bigint,
		signatures                     bytea[],
		timestamp                      timestamp with time zone,
		memo                           text,
		timeout_height                 bigint,
		extension_options              text[],
		non_critical_extension_options text[],
		auth_info_id                   bigint,
		tx_response_id                 bigint
	);
	
	create unique index idx_txes_hash
		on txes (hash);`
	migrations = append(migrations, queryTxes)

	queryBlocks := `create table blocks
	(
		id                       bigserial primary key,
		time_stamp               timestamp with time zone,
		height                   bigint,
		chain_id                 bigint,
		proposer_cons_address_id bigint,
		tx_indexed               boolean,
		block_events_indexed     boolean,
		block_hash               text
	)`
	migrations = append(migrations, queryBlocks)

	queryFees := `create table fees
		(
			id               bigserial primary key,
			tx_id            bigint,
			amount           numeric(78),
			denomination_id  bigint,
			payer_address_id bigint
		);`
	migrations = append(migrations, queryFees)

	queryTxResponses := `create table tx_responses
		(
			id         bigserial primary key,
			tx_hash    text,
			height     text,
			time_stamp text,
			code       bigint,
			raw_log    bytea,
			gas_used   bigint,
			gas_wanted bigint,
			codespace  text,
			data       text,
			info       text
		);`
	migrations = append(migrations, queryTxResponses)

	queryTxAuthInfo := `create table tx_auth_info
			(
				id     bigserial primary key,
				fee_id bigint,
				tip_id bigint
			);`
	migrations = append(migrations, queryTxAuthInfo)

	queryTxSignerInfos := `create table tx_signer_infos
		(
			auth_info_id   bigint not null,
			signer_info_id bigint not null,
			primary key (auth_info_id, signer_info_id)
		);`
	migrations = append(migrations, queryTxSignerInfos)

	queryTxSignerInfo := `create table tx_signer_info
	(
		id         bigserial primary key,
		address_id bigint,
		mode_info  text,
		sequence   bigint
	);`
	migrations = append(migrations, queryTxSignerInfo)

	queryAddresses := `create table addresses
			(
				id      bigserial primary key,
				address text
			);`
	migrations = append(migrations, queryAddresses)

	querySignerAddresses := `create table tx_signer_addresses
			(
				tx_id      bigint not null,
				address_id bigint not null,
				primary key (tx_id, address_id)
			);`
	migrations = append(migrations, querySignerAddresses)

	queryAuthFee := `create table tx_auth_info_fee
			(
				id        bigserial primary key,
				gas_limit bigint,
				payer     text,
				granter   text
			);`
	migrations = append(migrations, queryAuthFee)

	queryTxTip := `create table tx_tip
				(
					id     bigserial
						primary key,
					tipper text
				);`
	migrations = append(migrations, queryTxTip)

	queryDenoms := `create table denoms
		(
			id   bigserial
				primary key,
			base text
		);`
	migrations = append(migrations, queryDenoms)

	queryBlockSignatures := `create table block_signatures
		(
			id   bigserial
				primary key,
			block_id  bigint,
			validator_address text,
			timestamp timestamp with time zone
		);`
	migrations = append(migrations, queryBlockSignatures)

	queryMsgTypes := `create table message_types
		(
			id           bigserial primary key,
			message_type text not null
		);
		
		create unique index idx_message_types_message_type
			on public.message_types (message_type);
`
	migrations = append(migrations, queryMsgTypes)

	queryMsgEventTypes := `create table message_event_types
		(
			id   bigserial
				primary key,
			type text
		);
		
		create unique index idx_message_event_types_type
			on public.message_event_types (type);
		`
	migrations = append(migrations, queryMsgEventTypes)

	queryMessages := `create table messages
(
    id              bigserial
        primary key,
    tx_id           bigint
        constraint fk_messages_tx
            references public.txes,
    message_type_id bigint
        constraint fk_messages_message_type
            references public.message_types,
    message_index   bigint,
    message_bytes   bytea
);

create unique index "messageIndex"
    on public.messages (tx_id, message_index);

`
	migrations = append(migrations, queryMessages)

	queryMessageEvents := `create table public.message_events
(
    id                    bigserial
        primary key,
    index                 bigint,
    message_id            bigint
        constraint fk_message_events_message
            references public.messages,
    message_event_type_id bigint
        constraint fk_message_events_message_event_type
            references public.message_event_types
);

create unique index "messageEventIndex"
    on public.message_events (message_id, index);
`
	migrations = append(migrations, queryMessageEvents)

	queryMsgAttrKeys := `create table message_event_attribute_keys
(
    id  bigserial primary key,
    key text
);

create unique index idx_message_event_attribute_keys_key
    on public.message_event_attribute_keys (key);

`
	migrations = append(migrations, queryMsgAttrKeys)

	queryMsgEventAttrs := `create table message_event_attributes
(
    id                             bigserial
        primary key,
    message_event_id               bigint
        constraint fk_message_event_attributes_message_event
            references public.message_events,
    value                          text,
    index                          bigint,
    message_event_attribute_key_id bigint
        constraint fk_message_event_attributes_message_event_attribute_key
            references public.message_event_attribute_keys
);
create unique index "messageAttributeIndex"
    on message_event_attributes (message_event_id, index);
`
	migrations = append(migrations, queryMsgEventAttrs)

	queryTxDelegates := `create table tx_delegate_aggregateds
(
    hash         text,
    tx_type      text,
    timestamp    timestamp with time zone,
    validator    text,
    block_height bigint,
    amount       numeric(78),
    denom        text,
    sender       text,
    id           bigserial
);
`
	migrations = append(migrations, queryTxDelegates)

	queryVotesNormalised := `create table votes_normalized
(
    hash         text,
    weight       text,
    proposal_id text,
    height 		 bigint,
    timestamp    timestamp with time zone,
    option       text,
    voter        text
);`
	migrations = append(migrations, queryVotesNormalised)

	queryDepositsNormalised := `create table depositors_normalized
(	
    id 		 	 bigint,
    timestamp    timestamp with time zone,
    hash         text,
    height 		 bigint,
    sender       text,
    proposal_id  text,
    amount       numeric,
    denom        text
);`
	migrations = append(migrations, queryDepositsNormalised)

	queryCreateTxNormalized := `
		CREATE TABLE IF NOT EXISTS transactions_normalized (
			account TEXT,
			time TIMESTAMP WITH TIME ZONE
		);
	`
	migrations = append(migrations, queryCreateTxNormalized)

	for _, query := range migrations {
		_, err := postgresConn.Exec(ctx, query)
		if err != nil {
			log.Err(err).Msgf("couldn't manual postgres migration: %s", err.Error())
			return
		}
	}
}
