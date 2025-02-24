package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/noders-team/cosmos-indexer/config"
	"github.com/noders-team/cosmos-indexer/db"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

var (
	cfgFile string // config file location to load
	rootCmd = &cobra.Command{
		Use:   "cosmos-indexer",
		Short: "A CLI tool for indexing and querying on-chain data",
		Long: `Cosmos Tax CLI is a CLI tool for indexing and querying Cosmos-based blockchains,
		with a heavy focus on taxable events.`,
	}
	viperConf = viper.New()
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(getViperConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cosmos-indexer/config.yaml)")
}

func getViperConfig() {
	v := viper.New()

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		v.SetConfigType("toml")
	} else {
		// Check in current working dir
		pwd, err := os.Getwd()
		if err != nil {
			log.Fatal().Msgf("Could not determine current working dir. Err: %v", err)
		}
		if _, err := os.Stat(fmt.Sprintf("%v/config.toml", pwd)); err == nil {
			cfgFile = pwd
		} else {
			// file not in current working dir. Check home dir instead
			// Find home directory.
			home, err := os.UserHomeDir()
			if err != nil {
				log.Fatal().Msgf("Failed to find user home dir. Err: %v", err)
			}
			cfgFile = fmt.Sprintf("%s/.cosmos-indexer", home)
		}
		v.AddConfigPath(cfgFile)
		v.SetConfigType("toml")
		v.SetConfigName("config")
	}

	// Load defaults into a file at $HOME?
	var noConfig bool
	err := v.ReadInConfig()
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "Config File \"config\" Not Found"):
			noConfig = true
		case strings.Contains(err.Error(), "incomplete number"):
			log.Fatal().Msgf("Failed to read config file %v. This usually means you forgot to wrap a string in quotes.", err)
		default:
			log.Fatal().Msgf("Failed to read config file. Err: %v", err)
		}
	}

	if !noConfig {
		log.Info().Msgf("CFG successfully read from: ", cfgFile)
	}

	viperConf = v
}

// Set config vars from cpnfig file not already specified on command line.
func bindFlags(cmd *cobra.Command, v *viper.Viper) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		configName := f.Name

		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if !f.Changed && v.IsSet(configName) {
			val := v.Get(configName)
			err := cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
			if err != nil {
				log.Fatal().Msgf("Failed to bind config file value %v. Err: %v", configName, err)
			}
		}
	})
}

func setupLogger(logLevel string, logPath string, prettyLogging bool) {
	config.DoConfigureLogger(logPath, logLevel, prettyLogging)
}

func connectToDBAndMigrate(dbConfig config.Database) (*gorm.DB, error) {
	database, err := db.PostgresDbConnect(dbConfig.Host, dbConfig.Port, dbConfig.Database, dbConfig.User, dbConfig.Password, strings.ToLower(dbConfig.LogLevel))
	if err != nil {
		log.Err(err).Msg("Could not establish connection to the database")
		return nil, err
	}

	sqldb, _ := database.DB()
	sqldb.SetMaxIdleConns(50)
	sqldb.SetMaxOpenConns(200)
	sqldb.SetConnMaxLifetime(time.Hour)

	if indexer.cfg.Base.Mode == "fetcher" {
		log.Info().Msg("Skipping DB migrations for fetcher mode")
	} else {
		err = db.MigrateModels(database)
		if err != nil {
			log.Err(err).Msg("Error running DB migrations")
			return nil, err
		}
	}

	return database, err
}
