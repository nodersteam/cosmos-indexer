package cmd

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm/clause"

	"github.com/noders-team/cosmos-indexer/clients"
	"github.com/noders-team/cosmos-indexer/core/tx"
	"github.com/noders-team/cosmos-indexer/pkg/consumer"
	"github.com/noders-team/cosmos-indexer/pkg/model"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noders-team/cosmos-indexer/pkg/repository"
	"github.com/noders-team/cosmos-indexer/pkg/server"
	"github.com/noders-team/cosmos-indexer/pkg/service"
	blocks "github.com/noders-team/cosmos-indexer/proto"
	"google.golang.org/grpc"

	"github.com/nodersteam/probe/client"

	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/core"
	dbTypes "github.com/noders-team/cosmos-indexer/db"
	"github.com/noders-team/cosmos-indexer/db/models"
	"github.com/noders-team/cosmos-indexer/filter"
	"github.com/noders-team/cosmos-indexer/parsers"
	"github.com/noders-team/cosmos-indexer/probe"
	"github.com/spf13/cobra"

	probeClient "github.com/nodersteam/probe/client"

	"gorm.io/gorm"
)

const (
	modeFetcher = "fetcher"
)

type Indexer struct {
	cfg                                 *config.IndexConfig
	db                                  *gorm.DB
	cl                                  *client.ChainClient
	blockEventFilterRegistries          blockEventFilterRegistries
	messageTypeFilters                  []filter.MessageTypeFilter
	customBeginBlockEventParserRegistry map[string][]parsers.BlockEventParser // Used for associating parsers to block event types in BeginBlock events
	customEndBlockEventParserRegistry   map[string][]parsers.BlockEventParser // Used for associating parsers to block event types in EndBlock events
	customBeginBlockParserTrackers      map[string]models.BlockEventParser    // Used for tracking block event parsers in the database
	customEndBlockParserTrackers        map[string]models.BlockEventParser    // Used for tracking block event parsers in the database
	customModels                        []any
	rpcClient                           clients.ChainRPC
	txParser                            tx.Parser
}

type blockEventFilterRegistries struct {
	beginBlockEventFilterRegistry *filter.StaticBlockEventFilterRegistry
	endBlockEventFilterRegistry   *filter.StaticBlockEventFilterRegistry
}

var indexer Indexer

func init() {
	indexer.cfg = &config.IndexConfig{}
	config.SetupLogFlags(&indexer.cfg.Log, indexCmd)
	config.SetupDatabaseFlags(&indexer.cfg.Database, indexCmd)
	config.SetupProbeFlags(&indexer.cfg.Probe, indexCmd)
	config.SetupServerFlags(&indexer.cfg.Server, indexCmd)
	config.SetupThrottlingFlag(&indexer.cfg.Base.Throttling, indexCmd)
	config.SetupIndexSpecificFlags(indexer.cfg, indexCmd)
	config.SetupRedisFlags(&indexer.cfg.RedisConf, indexCmd)
	config.SetupMongoDBFlags(&indexer.cfg.MongoConf, indexCmd)

	rootCmd.AddCommand(indexCmd)
}

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Indexes the blockchain according to the configuration defined.",
	Long: `Indexes the Cosmos-based blockchain according to the configurations found on the command line
	or in the specified config file. Indexes taxable events into a database for easy querying. It is
	highly recommended to keep this command running as a background service to keep your index up to date.`,
	PreRunE: setupIndex,
	Run:     index,
}

func setupIndex(cmd *cobra.Command, args []string) error {
	bindFlags(cmd, viperConf)

	err := indexer.cfg.Validate()
	if err != nil {
		return err
	}

	ignoredKeys := config.CheckSuperfluousIndexKeys(viperConf.AllKeys())

	if len(ignoredKeys) > 0 {
		config.Log.Warnf("Warning, the following invalid keys will be ignored: %v", ignoredKeys)
	}

	setupLogger(indexer.cfg.Log.Level, indexer.cfg.Log.Path, indexer.cfg.Log.Pretty)

	// 0 is an invalid starting block, set it to 1
	if indexer.cfg.Base.StartBlock == 0 {
		indexer.cfg.Base.StartBlock = 1
	}

	db, err := connectToDBAndMigrate(indexer.cfg.Database)
	if err != nil {
		config.Log.Fatal("error migrating DB", err)
	}

	indexer.db = db

	indexer.blockEventFilterRegistries = blockEventFilterRegistries{
		beginBlockEventFilterRegistry: &filter.StaticBlockEventFilterRegistry{},
		endBlockEventFilterRegistry:   &filter.StaticBlockEventFilterRegistry{},
	}

	if indexer.cfg.Base.FilterFile != "" {
		f, err := os.Open(indexer.cfg.Base.FilterFile)
		if err != nil {
			config.Log.Fatalf("Failed to open block event filter file %s: %s", indexer.cfg.Base.FilterFile, err)
		}

		b, err := io.ReadAll(f)
		if err != nil {
			config.Log.Fatal("Failed to parse block event filter config", err)
		}

		indexer.blockEventFilterRegistries.beginBlockEventFilterRegistry.BlockEventFilters,
			indexer.blockEventFilterRegistries.beginBlockEventFilterRegistry.RollingWindowEventFilters,
			indexer.blockEventFilterRegistries.endBlockEventFilterRegistry.BlockEventFilters,
			indexer.blockEventFilterRegistries.endBlockEventFilterRegistry.RollingWindowEventFilters,
			indexer.messageTypeFilters,
			err = config.ParseJSONFilterConfig(b)
		if err != nil {
			config.Log.Fatal("Failed to parse block event filter config", err)
		}

	}

	if len(indexer.customModels) != 0 {
		err = dbTypes.MigrateInterfaces(indexer.db, indexer.customModels)
		if err != nil {
			config.Log.Fatal("Failed to migrate custom models", err)
		}
	}

	if len(indexer.customBeginBlockParserTrackers) != 0 {
		err = dbTypes.FindOrCreateCustomParsers(indexer.db, indexer.customBeginBlockParserTrackers)
		if err != nil {
			config.Log.Fatal("Failed to migrate custom block event parsers", err)
		}
	}

	if len(indexer.customEndBlockParserTrackers) != 0 {
		err = dbTypes.FindOrCreateCustomParsers(indexer.db, indexer.customEndBlockParserTrackers)
		if err != nil {
			config.Log.Fatal("Failed to migrate custom block event parsers", err)
		}
	}
	return nil
}

func setupIndexer() *Indexer {
	var err error

	// Setup chain specific stuff
	tx.ChainSpecificMessageTypeHandlerBootstrap(indexer.cfg.Probe.ChainID)

	config.SetChainConfig(indexer.cfg.Probe.AccountPrefix)

	indexer.cl = probe.GetProbeClient(indexer.cfg.Probe)
	indexer.rpcClient = clients.NewChainRPC(indexer.cl)

	// Depending on the app configuration, wait for the chain to catch up
	chainCatchingUp, err := indexer.rpcClient.IsCatchingUp()
	for indexer.cfg.Base.WaitForChain && chainCatchingUp && err == nil {
		// Wait between status checks, don't spam the node with requests
		config.Log.Debug("Chain is still catching up, please wait or disable check in config.")
		time.Sleep(time.Second * time.Duration(indexer.cfg.Base.WaitForChainDelay))
		chainCatchingUp, err = indexer.rpcClient.IsCatchingUp()

		// This EOF error pops up from time to time and is unpredictable
		// It is most likely an error on the node, we would need to see any error logs on the node side
		// Try one more time
		if err != nil && strings.HasSuffix(err.Error(), "EOF") {
			time.Sleep(time.Second * time.Duration(indexer.cfg.Base.WaitForChainDelay))
			chainCatchingUp, err = indexer.rpcClient.IsCatchingUp()
		}
	}
	if err != nil {
		config.Log.Fatal("Error querying chain status.", err)
	}

	txParser := tx.NewParser(indexer.db, indexer.cl, tx.NewProcessor(indexer.cl))
	indexer.txParser = txParser

	return &indexer
}

func calculateGoroutines(startBlock, endBlock, steps int64) (int64, []int64) {
	totalBlocks := endBlock - startBlock + 1
	numGoroutines := (totalBlocks + steps - 1) / steps

	blocksPerGoroutine := make([]int64, numGoroutines)

	for i := int64(0); i < numGoroutines; i++ {
		if totalBlocks >= steps {
			blocksPerGoroutine[i] = steps
			totalBlocks -= steps
		} else {
			blocksPerGoroutine[i] = totalBlocks
		}
	}

	return numGoroutines, blocksPerGoroutine
}

func index(_ *cobra.Command, _ []string) {
	idxr := setupIndexer()

	dbConn, err := idxr.db.DB()
	if err != nil {
		config.Log.Fatal("Failed to connect to DB", err)
	}
	defer dbConn.Close()

	ctx := context.Background()
	defer ctx.Done()

	switch idxr.cfg.Base.Mode {
	case modeFetcher:
		if idxr.cfg.Base.ModeTopics == nil {
			defaultTopics := []string{"indexer"}
			idxr.cfg.Base.ModeTopics = &defaultTopics
		}
		if idxr.cfg.Base.EndBlock < idxr.cfg.Base.StartBlock {
			panic("end-block must be higher then start-block for fetcher mode")
		}
		if idxr.cfg.Base.ModeBlocksStep == 0 {
			idxr.cfg.Base.ModeBlocksStep = 5000
		}

		runIndexerAsFetcher(ctx, idxr, idxr.cfg.Base.StartBlock, idxr.cfg.Base.EndBlock)
	default:
		if idxr.cfg.Base.ModeTopics == nil {
			defaultTopics := []string{"indexer"}
			idxr.cfg.Base.ModeTopics = &defaultTopics
		}
		runIndexer(ctx, idxr, idxr.cfg.Base.StartBlock, idxr.cfg.Base.EndBlock)
	}

	log.Info().Msgf("====EXITED=====")
}

func runIndexerAsFetcher(ctx context.Context, idxr *Indexer, startBlock, endBlock int64) {
	// blockChans are just the block heights; limit max jobs in the queue, otherwise this queue would contain one
	// item (block height) for every block on the entire blockchain we're indexing. Furthermore, once the queue
	// is close to empty, we will spin up a new thread to fill it up with new jobs.
	blockEnqueueChan := make(chan *core.EnqueueData, 1000000)

	// This channel represents query job results for the RPC queries to Cosmos Nodes. Every time an RPC query
	// completes, the query result will be sent to this channel (for later processing by a different thread).
	// Realistically, I expect that RPC queries will be slower than our relational DB on the local network.
	// If RPC queries are faster than DB inserts this buffer will fill up.
	// We will periodically check the buffer size to monitor performance so we can optimize later.
	rpcQueryThreads := int(idxr.cfg.Base.RPCWorkers)
	if rpcQueryThreads == 0 {
		rpcQueryThreads = 64
	}

	chain := models.Chain{
		ChainID: idxr.cfg.Probe.ChainID,
		Name:    idxr.cfg.Probe.ChainName,
	}

	dbChainID, err := dbTypes.GetDBChainID(idxr.db, chain)
	if err != nil {
		config.Log.Fatal("Failed to add/create chain in DB", err)
	}

	// This block consolidates all base RPC requests into one worker.
	// Workers read from the enqueued blocks and query blockchain data from the RPC server.
	var blockRPCWaitGroup sync.WaitGroup
	blockRPCWorkerDataChan := make(chan core.IndexerBlockEventData, 10000)

	worker := core.NewBlockRPCWorker(
		idxr.cfg.Probe.ChainID,
		idxr.cfg,
		idxr.cl,
		idxr.db,
		idxr.rpcClient,
	)

	for i := 0; i < rpcQueryThreads; i++ {
		blockRPCWaitGroup.Add(1)
		go worker.Worker(&blockRPCWaitGroup, blockEnqueueChan, blockRPCWorkerDataChan)
	}

	go func() {
		blockRPCWaitGroup.Wait()
		close(blockRPCWorkerDataChan)
	}()

	// setup cache
	rdb := redis.NewClient(&redis.Options{
		Addr:     idxr.cfg.RedisConf.RedisAddr,
		Password: idxr.cfg.RedisConf.RedisPsw,
		DB:       0, // use default DB
	})

	go func(ctx context.Context) {
		blockCounter := 0
		for newBlock := range blockRPCWorkerDataChan {
			if blockCounter >= idxr.cfg.Base.ModeCoolDownCount {
				log.Info().Msgf("hit the block counter, sleeping.... %d (in memory waiting)", len(blockRPCWorkerDataChan))
				time.Sleep(time.Duration(idxr.cfg.Base.ModeCoolDownMins) * time.Minute)
				blockCounter = 0
			}

			bl, err := newBlock.MarshalJSON(&idxr.cl.Codec)
			if err != nil {
				log.Err(err).Msgf("💩error marshalling block")
				continue
			}

			encoded := base64.StdEncoding.EncodeToString(bl)
			err = rdb.Publish(ctx, roundRobinTopic(idxr), encoded).Err()
			if err != nil {
				log.Err(err).Msgf("💩error publishing block")
				continue
			}
			log.Info().Msgf("🤌block %d published successfully, counter %d", newBlock.BlockData.Block.Height, blockCounter)
			blockCounter++
		}
	}(ctx)

	ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = rdb.Ping(ctxPing).Err(); err != nil {
		panic(err)
	}

	log.Info().Msgf("running indexer as fetcher mode.")
	steps := idxr.cfg.Base.ModeBlocksStep

	numGoroutines, blocks := calculateGoroutines(startBlock, endBlock, steps)
	counter := startBlock
	log.Info().Msgf("num go routines for indexing %d", numGoroutines)
	for i := int64(0); i < numGoroutines; i++ {
		blocksToProceed := blocks[i]
		endBlockInternal := counter + blocksToProceed

		log.Info().Msgf("🚀starting indexer for blocks %d - %d", counter, endBlockInternal)
		allBlocks := make(map[int64]struct{})
		bls, err := dbTypes.GetBlocksInRange(idxr.db, dbChainID, counter, endBlockInternal)
		if err == nil {
			for _, b := range bls {
				allBlocks[b.Height] = struct{}{}
			}
		}
		log.Info().Msgf("Already indexed blocks in the interval: %d", len(allBlocks))
		blockEnqueueFunction, err := core.GenerateDefaultEnqueueFunction(idxr.db, *idxr.cfg, dbChainID,
			idxr.rpcClient, counter, endBlockInternal, allBlocks)
		if err != nil {
			config.Log.Fatal("Failed to generate block enqueue function", err)
		}
		go func() {
			err = blockEnqueueFunction(blockEnqueueChan)
			if err != nil {
				config.Log.Fatal("Block enqueue failed", err)
			}
		}()

		counter += blocksToProceed
	}

	<-ctx.Done() // TODO find better place

	close(blockEnqueueChan)
	log.Info().Msgf("closing.")
}

func roundRobinTopic(idxr *Indexer) string {
	topics := *idxr.cfg.Base.ModeTopics
	return topics[int(time.Now().UnixNano())%len(topics)]
}

func runIndexer(ctx context.Context, idxr *Indexer, startBlock, endBlock int64) {
	// blockChans are just the block heights; limit max jobs in the queue, otherwise this queue would contain one
	// item (block height) for every block on the entire blockchain we're indexing. Furthermore, once the queue
	// is close to empty, we will spin up a new thread to fill it up with new jobs.
	blockEnqueueChan := make(chan *core.EnqueueData, 100000)

	// This channel represents query job results for the RPC queries to Cosmos Nodes. Every time an RPC query
	// completes, the query result will be sent to this channel (for later processing by a different thread).
	// Realistically, I expect that RPC queries will be slower than our relational DB on the local network.
	// If RPC queries are faster than DB inserts this buffer will fill up.
	// We will periodically check the buffer size to monitor performance so we can optimize later.
	rpcQueryThreads := int(idxr.cfg.Base.RPCWorkers)
	if rpcQueryThreads == 0 {
		rpcQueryThreads = 64
	}

	var wg sync.WaitGroup // This group is to ensure we are done processing transactions and events before returning

	chain := models.Chain{
		ChainID: idxr.cfg.Probe.ChainID,
		Name:    idxr.cfg.Probe.ChainName,
	}

	dbChainID, err := dbTypes.GetDBChainID(idxr.db, chain)
	if err != nil {
		config.Log.Fatal("Failed to add/create chain in DB", err)
	}

	// This block consolidates all base RPC requests into one worker.
	// Workers read from the enqueued blocks and query blockchain data from the RPC server.
	var blockRPCWaitGroup sync.WaitGroup
	blockRPCWorkerDataChan := make(chan core.IndexerBlockEventData)

	worker := core.NewBlockRPCWorker(
		idxr.cfg.Probe.ChainID,
		idxr.cfg,
		idxr.cl,
		idxr.db,
		idxr.rpcClient,
	)

	for i := 0; i < rpcQueryThreads; i++ {
		blockRPCWaitGroup.Add(1)
		go worker.Worker(&blockRPCWaitGroup, blockEnqueueChan, blockRPCWorkerDataChan)
	}

	go func() {
		blockRPCWaitGroup.Wait()
		close(blockRPCWorkerDataChan)
	}()

	// Block BeginBlocker and EndBlocker indexing requirements. Indexes block events that took place in the BeginBlock and EndBlock state transitions
	blockEventsDataChan := make(chan *blockEventsDBData, 4*rpcQueryThreads)
	txDataChan := make(chan *dbData, 4*rpcQueryThreads)

	dbDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		idxr.cfg.Database.User, idxr.cfg.Database.Password, idxr.cfg.Database.Host, idxr.cfg.Database.Port, idxr.cfg.Database.Database)
	dbConnRepo, err := connectPgxPool(ctx, dbDSN)
	if err != nil {
		config.Log.Fatal("Error connecting DB", err)
	}
	repoBlocks := repository.NewBlocks(dbConnRepo)
	srvBlocks := service.NewBlocks(repoBlocks)

	repoTxs := repository.NewTxs(dbConnRepo)
	srvTxs := service.NewTxs(repoTxs)

	// setup mongoDB
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(idxr.cfg.MongoConf.MongoAddr))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = mongoClient.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	err = mongoClient.Ping(ctx, &readpref.ReadPref{})
	if err != nil {
		panic(err)
	}

	db := mongoClient.Database(idxr.cfg.MongoConf.MongoDB)
	searchRepo := repository.NewSearch(db)
	srvSearch := service.NewSearch(searchRepo)

	// setup cache
	rdb := redis.NewClient(&redis.Options{
		Addr:     idxr.cfg.RedisConf.RedisAddr,
		Password: idxr.cfg.RedisConf.RedisPsw,
		DB:       0, // use default DB
	})

	ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = rdb.Ping(ctxPing).Err(); err != nil {
		panic(err)
	}
	cache := repository.NewCache(rdb)

	log.Info().Msgf("running Blocks server %d", idxr.cfg.Server.Port)
	grpcServURL := fmt.Sprintf(":%d", idxr.cfg.Server.Port)
	listener, err := net.Listen("tcp", grpcServURL)
	if err != nil {
		config.Log.Fatal("Unable to run listener", err)
	}

	srvAggregates := service.NewAggregates(cache, repoBlocks, repoTxs)
	_, err = srvAggregates.StoreAggregates(ctx)
	if err != nil {
		log.Err(err).Msgf("error storing aggregates")
		panic(err)
	}

	blocksServer := server.NewBlocksServer(srvBlocks, srvTxs, srvSearch, *cache, srvAggregates)
	size := 1024 * 1024 * 50
	grpcServer := grpc.NewServer(
		grpc.MaxSendMsgSize(size),
		grpc.MaxRecvMsgSize(size))
	blocks.RegisterBlocksServiceServer(grpcServer, blocksServer)
	go func() {
		log.Info().Msgf("blocks server started: " + grpcServURL)
		if err = grpcServer.Serve(listener); err != nil {
			grpcServer.GracefulStop()
			return
		}
	}()

	chBlocks := make(chan *model.BlockInfo, 1000)
	defer close(chBlocks)
	chTxs := make(chan *model.Tx, 1000)
	defer close(chTxs)

	cacheConsumer := consumer.NewCacheConsumer(cache, chBlocks, chTxs, cache)
	go func(ctx context.Context) {
		err = cacheConsumer.RunBlocks(ctx)
		if err != nil {
			log.Err(err).Msg("Error running cache: RunBlocks")
		}
	}(ctx)

	go func(ctx context.Context) {
		err := cacheConsumer.RunTransactions(ctx)
		if err != nil {
			log.Err(err).Msg("Error running cache: RunTransactions")
		}
	}(ctx)

	aggregatesConsumer := consumer.NewAggregatesConsumer(repoTxs, srvAggregates)
	go func(ctx context.Context) {
		err := aggregatesConsumer.Consume(ctx)
		if err != nil {
			log.Err(err).Msg("Error running aggregates: Consume")
		}
	}(ctx)

	go func(ctx context.Context) {
		err := aggregatesConsumer.RefreshMaterializedViews(ctx)
		if err != nil {
			log.Err(err).Msgf("Error refreshing materialized views")
		}
	}(ctx)

	// pipe between fetcher and reader
	pipe := make(chan core.IndexerBlockEventData)
	topic := ""
	if idxr.cfg.Base.ModeTopics != nil {
		topics := *idxr.cfg.Base.ModeTopics
		topic = topics[0]
	}

	go runPipe(ctx, idxr, rdb, topic, pipe, blockRPCWorkerDataChan)

	wg.Add(1)
	go idxr.processBlocks(
		&wg,
		core.HandleFailedBlock,
		pipe,
		blockEventsDataChan,
		txDataChan,
		dbChainID,
		indexer.blockEventFilterRegistries,
		chBlocks,
		func() *repository.Cache {
			return cache
		}(),
		&indexer.cl.Codec)
	wg.Add(1)
	go idxr.doDBUpdates(&wg, txDataChan, blockEventsDataChan, chTxs, repoTxs, cache)

	// search index
	txSearchConsumer := consumer.NewSearchTxConsumer(rdb, "pub/txs", searchRepo) // TODO
	go func() {
		err = txSearchConsumer.Consume(ctx)
		if err != nil {
			log.Err(err).Msgf("error on tx search consumer closing")
		}
	}()

	blSearchConsumer := consumer.NewSearchBlocksConsumer(rdb, "pub/blocks", searchRepo) // TODO
	go func(ctx context.Context) {
		err := blSearchConsumer.Consume(ctx)
		if err != nil {
			log.Err(err).Msgf("error on tx search consumer closing")
		}
	}(ctx)

	// migration
	migrator := dbTypes.NewLocalMigrator(db, dbConnRepo, searchRepo, repoTxs)
	go func() {
		log.Info().Msgf("Starting migration")
		db, err = migrator.Migrate(ctx)
		if err != nil {
			log.Err(err).Msgf("Migration failed")
			panic(err)
		}
		log.Info().Msgf("Migration completed")
	}()

	// for genesis indexing running only blocks below --start.block
	emptyBl := make(map[int64]struct{})
	var blockEnqueueFunction func(chan *core.EnqueueData) error
	switch {
	// Default block enqueue functions based on config values
	case idxr.cfg.Base.ReindexMessageType != "":
		blockEnqueueFunction, err = core.GenerateMsgTypeEnqueueFunction(idxr.db, *idxr.cfg, dbChainID,
			idxr.cfg.Base.ReindexMessageType, startBlock, endBlock)
		if err != nil {
			config.Log.Fatal("Failed to generate block enqueue function", err)
		}
	case idxr.cfg.Base.BlockInputFile != "":
		blockEnqueueFunction, err = core.GenerateBlockFileEnqueueFunction(*idxr.cfg, idxr.cfg.Base.BlockInputFile, idxr.rpcClient)
		if err != nil {
			config.Log.Fatal("Failed to generate block enqueue function", err)
		}
	default:
		blockEnqueueFunction, err = core.GenerateDefaultEnqueueFunction(idxr.db, *idxr.cfg, dbChainID,
			idxr.rpcClient, startBlock, endBlock, emptyBl)
		if err != nil {
			config.Log.Fatal("Failed to generate block enqueue function", err)
		}
	}

	err = blockEnqueueFunction(blockEnqueueChan)
	if err != nil {
		config.Log.Fatal("Block enqueue failed", err)
	}

	close(blockEnqueueChan)

	wg.Wait()
}

func runPipe(ctx context.Context, idxr *Indexer, rdb *redis.Client, topic string,
	pipe chan core.IndexerBlockEventData, blockRPCWorkerDataChan chan core.IndexerBlockEventData,
) {
	go func(ctx context.Context, rdb *redis.Client) {
		for {
			select {
			case <-ctx.Done():
				return
			case newBlock := <-blockRPCWorkerDataChan:
				bl, err := newBlock.MarshalJSON(&idxr.cl.Codec)
				if err != nil {
					log.Err(err).Msgf("💩error marshalling block")
					continue
				}
				encoded := base64.StdEncoding.EncodeToString(bl)

				err = rdb.Publish(ctx, topic, encoded).Err()
				if err != nil {
					log.Err(err).Msgf("💩error publishing block")
					continue
				}
				log.Info().Msgf("🤌block %d published successfully", newBlock.BlockData.Block.Height)
			}
		}
	}(ctx, rdb)

	subscriber := rdb.Subscribe(ctx, topic)
	defer func() {
		err := subscriber.Unsubscribe(ctx, topic)
		if err != nil {
			log.Error().Err(err).Msg("unsubscribe pubsub")
		}
		err = subscriber.Close()
		if err != nil {
			log.Error().Err(err).Msg("close pubsub")
		}
	}()

	innerReceiver := make(chan core.IndexerBlockEventData)
	defer close(innerReceiver)

	go func(inner chan core.IndexerBlockEventData) {
		for {
			msg, err := subscriber.ReceiveMessage(ctx)
			if err != nil {
				log.Err(err).Msgf("error in subscriber.ReceiveMessage")
				continue
			}

			b, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil {
				log.Err(err).Msgf("error decoding")
				continue
			}
			var in core.IndexerBlockEventData
			err = in.UnmarshalJSON(b)
			if err != nil {
				log.Error().Msgf("error unmarshalling block")
				continue
			}

			log.Info().Msgf("💩  ==> received block for writing block %d.", in.BlockData.Block.Height)

			inner <- in
		}
	}(innerReceiver)

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msgf("breaking the worker loop.")
			return
		case newRecord := <-innerReceiver:
			pipe <- newRecord
		}
	}
}

// connectPgxPool establishes a connection to a PostgreSQL database.
func connectPgxPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	conn, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %v", err)
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %v", err)
	}

	return conn, nil
}

// GetIndexerStartingHeight will determine which block to start at
// if start block is set to -1, it will start at the highest block indexed
// otherwise, it will start at the first missing block between the start and end height
func (idxr *Indexer) GetIndexerStartingHeight(chainID uint) int64 {
	// If the start height is set to -1, resume from the highest block already indexed
	if idxr.cfg.Base.StartBlock == -1 {
		latestBlock, err := idxr.rpcClient.GetLatestBlockHeight()
		if err != nil {
			log.Err(err).Msgf("Error getting blockchain latest height. Err: %v", err)
		}

		fmt.Println("Found latest block", latestBlock)
		highestIndexedBlock := dbTypes.GetHighestIndexedBlock(idxr.db, chainID)
		if highestIndexedBlock.Height < latestBlock {
			return highestIndexedBlock.Height + 1
		}
	}

	// if we are re-indexing, just start at the configured start block
	if idxr.cfg.Base.ReIndex {
		return idxr.cfg.Base.StartBlock
	}

	maxStart := idxr.cfg.Base.EndBlock
	if maxStart == -1 {
		heighestBlock := dbTypes.GetHighestIndexedBlock(idxr.db, chainID)
		maxStart = heighestBlock.Height
	}

	// Otherwise, start at the first block after the configured start block that we have not yet indexed.
	return dbTypes.GetFirstMissingBlockInRange(idxr.db, idxr.cfg.Base.StartBlock, maxStart, chainID)
}

type dbData struct {
	txDBWrappers []dbTypes.TxDBWrapper
	block        models.Block
}

type blockEventsDBData struct {
	blockDBWrapper *dbTypes.BlockDBWrapper
}

// This function is responsible for processing raw RPC data into app-usable types. It handles both block events and transactions.
// It parses each dataset according to the application configuration requirements and passes the data to the channels that handle the parsed data.
func (idxr *Indexer) processBlocks(wg *sync.WaitGroup,
	failedBlockHandler core.FailedBlockHandler,
	blockRPCWorkerChan chan core.IndexerBlockEventData,
	blockEventsDataChan chan *blockEventsDBData,
	txDataChan chan *dbData,
	chainID uint,
	blockEventFilterRegistry blockEventFilterRegistries,
	blocksCh chan *model.BlockInfo,
	cache *repository.Cache,
	_ *probeClient.Codec,
) {
	defer close(blockEventsDataChan)
	defer close(txDataChan)
	defer wg.Done()

	for blockData := range blockRPCWorkerChan {
		currentHeight := blockData.BlockData.Block.Height
		config.Log.Infof("Parsing data for block %d", currentHeight)

		block, err := core.ProcessBlock(blockData.BlockData, blockData.BlockResultsData, chainID)
		if err != nil {
			config.Log.Error("ProcessBlock: unhandled error", err)
			failedBlockHandler(currentHeight, core.UnprocessableTxError, err)
			err := dbTypes.UpsertFailedBlock(idxr.db, currentHeight, idxr.cfg.Probe.ChainID, idxr.cfg.Probe.ChainName)
			if err != nil {
				config.Log.Fatal("Failed to insert failed block", err)
			}
			continue
		}

		config.Log.Info("Parsing block events")
		blockDBWrapper, err := core.ProcessRPCBlockResults(*indexer.cfg, block,
			blockData.BlockResultsData,
			indexer.customBeginBlockEventParserRegistry, indexer.customEndBlockEventParserRegistry)
		if err != nil {
			config.Log.Errorf("Failed to process block events during block %d event processing, adding to failed block events table", currentHeight)
			failedBlockHandler(currentHeight, core.FailedBlockEventHandling, err)
			err := dbTypes.UpsertFailedEventBlock(idxr.db, currentHeight, idxr.cfg.Probe.ChainID, idxr.cfg.Probe.ChainName)
			if err != nil {
				config.Log.Fatal("Failed to insert failed block event", err)
			}
		} else {
			config.Log.Infof("Finished parsing block event data for block %d", currentHeight)

			var beginBlockFilterError error
			var endBlockFilterError error
			if blockEventFilterRegistry.beginBlockEventFilterRegistry != nil && blockEventFilterRegistry.beginBlockEventFilterRegistry.NumFilters() > 0 {
				blockDBWrapper.BeginBlockEvents, beginBlockFilterError = core.FilterRPCBlockEvents(blockDBWrapper.BeginBlockEvents, *blockEventFilterRegistry.beginBlockEventFilterRegistry)
			}

			if blockEventFilterRegistry.endBlockEventFilterRegistry != nil && blockEventFilterRegistry.endBlockEventFilterRegistry.NumFilters() > 0 {
				blockDBWrapper.EndBlockEvents, endBlockFilterError = core.FilterRPCBlockEvents(blockDBWrapper.EndBlockEvents, *blockEventFilterRegistry.endBlockEventFilterRegistry)
			}

			if beginBlockFilterError == nil && endBlockFilterError == nil {
				blockEventsDataChan <- &blockEventsDBData{
					blockDBWrapper: blockDBWrapper,
				}
			} else {
				config.Log.Errorf("Failed to filter block events during block %d event processing, adding to failed block events table. Begin blocker filter error %s. End blocker filter error %s", currentHeight, beginBlockFilterError, endBlockFilterError)
				failedBlockHandler(currentHeight, core.FailedBlockEventHandling, err)
				err := dbTypes.UpsertFailedEventBlock(idxr.db, currentHeight, idxr.cfg.Probe.ChainID, idxr.cfg.Probe.ChainName)
				if err != nil {
					config.Log.Fatal("Failed to insert failed block event", err)
				}
			}
		}

		if blockData.IndexTransactions && !blockData.TxRequestsFailed {
			config.Log.Info("Parsing transactions")
			var txDBWrappers []dbTypes.TxDBWrapper
			var err error

			if blockData.GetTxsResponse != nil && len(blockData.GetTxsResponse.Txs) > 0 {
				config.Log.Infof("Processing TXs from RPC TX Search response size: %d total %d",
					len(blockData.GetTxsResponse.Txs),
					blockData.GetTxsResponse.Total)
				txDBWrappers, _, err = idxr.txParser.ProcessRPCTXs(idxr.messageTypeFilters, blockData.GetTxsResponse)
			} else if blockData.BlockResultsData != nil {
				config.Log.Info("Processing TXs from BlockResults search response")
				txDBWrappers, _, err = idxr.txParser.ProcessRPCBlockByHeightTXs(idxr.messageTypeFilters, blockData.BlockData, blockData.BlockResultsData)
			}

			if err != nil {
				config.Log.Error("ProcessRpcTxs: unhandled error", err)
				failedBlockHandler(currentHeight, core.UnprocessableTxError, err)
				err := dbTypes.UpsertFailedBlock(idxr.db, currentHeight, idxr.cfg.Probe.ChainID, idxr.cfg.Probe.ChainName)
				if err != nil {
					config.Log.Fatal("Failed to insert failed block", err)
				}
			} else {
				txDataChan <- &dbData{
					txDBWrappers: txDBWrappers,
					block:        block,
				}
			}
		}
		blocksCh <- idxr.toBlockInfo(block)

		if cache != nil {
			if err = cache.PublishBlock(context.Background(), &block); err != nil {
				config.Log.Error("Failed to publish block info", err)
			}
		}
	}
}

func (idxr *Indexer) toBlockInfo(in models.Block) *model.BlockInfo {
	return &model.BlockInfo{
		BlockHeight:              in.Height,
		ProposedValidatorAddress: in.ProposerConsAddress.Address,
		TotalTx:                  int64(in.TotalTxs),
		GenerationTime:           in.TimeStamp,
		BlockHash:                in.BlockHash,
	}
}

// doDBUpdates will read the data out of the db data chan that had been processed by the workers
// if this is a dry run, we will simply empty the channel and track progress
// otherwise we will index the data in the DB.
// it will also read rewars data and index that.
func (idxr *Indexer) doDBUpdates(wg *sync.WaitGroup,
	txDataChan chan *dbData,
	blockEventsDataChan chan *blockEventsDBData,
	txsCh chan *model.Tx,
	txRepo repository.Txs,
	cache repository.PubSubCache,
) {
	blocksProcessed := 0
	dbWrites := 0
	dbReattempts := 0
	timeStart := time.Now()
	defer wg.Done()

	for {
		// break out of loop once all channels are fully consumed
		if txDataChan == nil && blockEventsDataChan == nil {
			config.Log.Info("DB updates complete")
			break
		}

		select {
		// read tx data from the data chan
		case data, ok := <-txDataChan:
			if !ok {
				txDataChan = nil
				continue
			}
			dbWrites++

			config.Log.Info(fmt.Sprintf("Indexing %v TXs from block %d", len(data.txDBWrappers), data.block.Height))
			_, _, err := dbTypes.IndexNewBlock(idxr.db, data.block, data.txDBWrappers, *idxr.cfg)
			if err != nil {
				// Do a single reattempt on failure
				dbReattempts++
				_, _, err = dbTypes.IndexNewBlock(idxr.db, data.block, data.txDBWrappers, *idxr.cfg)
				if err != nil {
					config.Log.Fatal(fmt.Sprintf("Error indexing block %v.", data.block.Height), err)
				}
			}

			// Just measuring how many blocks/second we can process
			if idxr.cfg.Base.BlockTimer > 0 {
				blocksProcessed++
				if blocksProcessed%int(idxr.cfg.Base.BlockTimer) == 0 {
					totalTime := time.Since(timeStart)
					config.Log.Info(fmt.Sprintf("Processing %d blocks took %f seconds. %d total blocks have been processed.\n", idxr.cfg.Base.BlockTimer, totalTime.Seconds(), blocksProcessed))
					timeStart = time.Now()
				}
				if float64(dbReattempts)/float64(dbWrites) > .1 {
					config.Log.Fatalf("More than 10%% of the last %v DB writes have failed.", dbWrites)
				}
			}

			for _, txDb := range data.txDBWrappers {
				transaction := txDb.Tx

				transaction.Block = data.block
				res, err := txRepo.GetSenderAndReceiver(context.Background(), transaction.Hash)
				if err != nil {
					log.Err(err).Msgf("unable to find sender and receiver for tx %s", transaction.Hash)
				} else {
					transaction.SenderReceiver = res
				}

				// TODO not the best place
				go func(tx model.Tx) {
					events, err := txRepo.GetEvents(context.Background(), tx.ID)
					if err != nil {
						log.Err(err).Msgf("Failed to get events for tx %s", tx.Hash)
					}
					errAggr := idxr.saveAggregated(context.Background(), txRepo, &tx, events)
					if errAggr != nil {
						log.Err(errAggr).Msgf("Failed to save aggregated for tx %s", tx.Hash)
					}

					errEvVals := idxr.saveAggregatedEventValues(context.Background(), &tx, events)
					if errEvVals != nil {
						log.Err(errEvVals).Msgf("Failed to save saveAggregatedEventValues for tx %s", tx.Hash)
					}

					errEv := idxr.saveAggregatedEvents(context.Background(), &tx, events)
					if errEv != nil {
						log.Err(errEv).Msgf("Failed to save saveAggregatedEvents for tx %s", tx.Hash)
					}
				}(transaction)

				// TODO decomposite everything
				txsCh <- &transaction
				// TODO don't publish old transactions
				if err := cache.PublishTx(context.Background(), &transaction); err != nil {
					config.Log.Error(err.Error())
				}
			}

		case eventData, ok := <-blockEventsDataChan:
			if !ok {
				blockEventsDataChan = nil
				continue
			}
			dbWrites++
			numEvents := len(eventData.blockDBWrapper.BeginBlockEvents) + len(eventData.blockDBWrapper.EndBlockEvents)
			config.Log.Info(fmt.Sprintf("Indexing %v Block Events from block %d", numEvents, eventData.blockDBWrapper.Block.Height))
			identifierLoggingString := fmt.Sprintf("block %d", eventData.blockDBWrapper.Block.Height)

			bl := dbTypes.GetBlockByHeight(idxr.db, eventData.blockDBWrapper.Block.Height)
			if bl.ID > 0 {
				log.Info().Msgf("Block already indexed %d, ignoring", eventData.blockDBWrapper.Block.Height)
				continue
			}

			indexedDataset, err := dbTypes.IndexBlockEvents(idxr.db, eventData.blockDBWrapper)
			if err != nil {
				config.Log.Fatal(fmt.Sprintf("Error indexing block events for %s.", identifierLoggingString), err)
			}

			err = dbTypes.IndexCustomBlockEvents(*idxr.cfg, idxr.db, indexedDataset, identifierLoggingString, idxr.customBeginBlockParserTrackers, idxr.customEndBlockParserTrackers)
			if err != nil {
				config.Log.Fatal(fmt.Sprintf("Error indexing custom block events for %s.", identifierLoggingString), err)
			}

			config.Log.Info(fmt.Sprintf("Finished indexing %v Block Events from block %d", numEvents, eventData.blockDBWrapper.Block.Height))
		}
	}
}

func (idxr *Indexer) saveAggregatedEventValues(ctx context.Context, tx *model.Tx, events []*model.TxEvents) error {
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		var txEvents model.TxEventsValsAggregated
		txEvents.TxHash = tx.Hash
		txEvents.MsgType = event.MessageType
		txEvents.EvAttrValue = fmt.Sprintf("%x", md5.Sum([]byte(strings.ToLower(event.Value))))
		txEvents.TxTimestamp = tx.Timestamp

		log.Debug().Msgf("trying tosave aggregated tx_events %v", txEvents)
		err := idxr.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&txEvents).Error
		})
		if err != nil {
			log.Err(err).Msgf("error saving aggregated tx_events %v", txEvents)
		}
	}

	return nil
}

func (idxr *Indexer) saveAggregatedEvents(ctx context.Context, tx *model.Tx, events []*model.TxEvents) error {
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		var txEvents model.TxEventsAggregated

		txEvents.TxHash = tx.Hash
		txEvents.MessageType = event.MessageType
		txEvents.MessageTypeIndex = event.Index
		txEvents.MessageEventType = event.Type
		txEvents.MessageEventAttrIndex = event.EventIndex
		txEvents.MessageEventAttrValue = event.Value
		txEvents.MessageEventAttrKey = event.Key

		log.Debug().Msgf("trying tosave aggregated saveAggregatedEvents %v", txEvents)
		err := idxr.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&txEvents).Error
		})
		if err != nil {
			log.Err(err).Msgf("error saving aggregated saveAggregatedEvents %v", txEvents)
		}
	}

	return nil
}

func (idxr *Indexer) saveAggregated(ctx context.Context, txRepo repository.Txs, tx *model.Tx, events []*model.TxEvents) error {
	if len(events) == 0 {
		return nil
	}

	var txDelegateAggregated model.TxDelegateAggregated

	txDelegateAggregated.Hash = tx.Hash
	txDelegateAggregated.Timestamp = tx.Timestamp
	txDelegateAggregated.BlockHeight = tx.Block.Height

	isMsgDelegate := false
	isMsgUndelegate := false
	for _, event := range events {
		if event.MessageType == "/cosmos.staking.v1beta1.MsgDelegate" {
			isMsgDelegate = true
		} else if event.MessageType == "/cosmos.staking.v1beta1.MsgUndelegate" {
			isMsgUndelegate = true
		}

		txDelegateAggregated.TxType = event.MessageType
		if event.Key == "validator" {
			txDelegateAggregated.Validator = event.Value
		}

		if event.Type == "coin_received" && event.Key == "amount" {
			amount, denom, err := txRepo.ExtractNumber(event.Value)
			if err != nil {
				return err
			}
			if isMsgDelegate {
				txDelegateAggregated.Amount = amount
			} else if isMsgUndelegate {
				txDelegateAggregated.Amount = amount.Neg()
			}
			txDelegateAggregated.Denom = denom
		}

		if event.Type == "coin_spent" && event.Key == "spender" {
			txDelegateAggregated.Sender = event.Value
		}
	}

	if isMsgDelegate || isMsgUndelegate {
		return idxr.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "hash"}},
				DoNothing: true,
			}).Where("hash = ?", txDelegateAggregated.Hash).FirstOrCreate(&txDelegateAggregated).Error
			if err != nil {
				log.Err(err).Msgf("error saving aggregated %v", txDelegateAggregated)
			}
			return err
		})
	}

	return nil
}
