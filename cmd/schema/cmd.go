package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	localViper "github.com/yandex-cloud/jaeger-ydb-store/internal/viper"
	"github.com/yandex-cloud/ydb-go-sdk/v2/scheme"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/cmd/schema/watcher"
	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.SetDefault("watcher_interval", time.Minute*5)
	viper.SetDefault("watcher_age", time.Hour*24)
	viper.SetDefault("watcher_lookahead", time.Hour*12)
	viper.SetDefault("parts_traces", 32)
	viper.SetDefault("parts_idx_tag", 32)
	viper.SetDefault("parts_idx_tag_v2", 32)
	viper.SetDefault("parts_idx_duration", 32)
	viper.SetDefault("parts_idx_service_name", 32)
	viper.SetDefault("parts_idx_service_op", 32)
	viper.SetDefault(db.KeyYDBPartitionSize, "1024mb")
	viper.AutomaticEnv()
}

func main() {
	localViper.ConfigureViperFromFlag(viper.GetViper())

	command := &cobra.Command{
		Use: "jaeger-ydb-schema",
	}
	command.PersistentFlags().String("address", "", "ydb host:port (env: YDB_ADDRESS)")
	command.PersistentFlags().String("path", "", "ydb path (env: YDB_PATH)")
	command.PersistentFlags().String("folder", "", "ydb folder (env: YDB_FOLDER)")
	command.PersistentFlags().String("token", "", "ydb oauth token (env: YDB_TOKEN)")
	command.PersistentFlags().String("config", "", "path to config file to configure Viper from")

	viper.BindPFlag("ydb_address", command.PersistentFlags().Lookup("address"))
	viper.BindPFlag("ydb_path", command.PersistentFlags().Lookup("path"))
	viper.BindPFlag("ydb_folder", command.PersistentFlags().Lookup("folder"))
	viper.BindPFlag("ydb_token", command.PersistentFlags().Lookup("token"))

	cfg := zap.NewProductionConfig()
	logger, err := cfg.Build()
	if err != nil {
		log.Fatal(err)
	}

	watcherCmd := &cobra.Command{
		Use: "watcher",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := watcher.Options{
				Expiration: viper.GetDuration("watcher_age"),
				Lookahead:  viper.GetDuration("watcher_lookahead"),
				DBPath: schema.DbPath{
					Path:   viper.GetString(db.KeyYdbPath),
					Folder: viper.GetString(db.KeyYdbFolder),
				},
			}
			if opts.Expiration == 0 {
				return fmt.Errorf("cannot use watcher age '%s'", opts.Expiration)
			}

			shutdown := make(chan os.Signal)
			signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
			tc, err := tableClient(viper.GetViper())
			if err != nil {
				return fmt.Errorf("failed to create table client: %w", err)
			}
			pool := &table.SessionPool{
				Builder: tc,
			}

			logger.Info("starting watcher")
			w := watcher.NewWatcher(opts, pool, logger)
			w.Run(viper.GetDuration("watcher_interval"))
			<-shutdown
			logger.Info("stopping watcher")
			return nil
		},
	}
	dropCmd := &cobra.Command{
		Use: "drop-tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			dbPath := schema.DbPath{
				Path:   viper.GetString(db.KeyYdbPath),
				Folder: viper.GetString(db.KeyYdbFolder),
			}
			tc, err := tableClient(viper.GetViper())
			if err != nil {
				return fmt.Errorf("failed to create table client: %w", err)
			}
			session, err := tc.CreateSession(ctx)
			if err != nil {
				return err
			}
			sc := scheme.Client{Driver: tc.Driver}
			d, err := sc.ListDirectory(ctx, dbPath.String())
			if err != nil {
				return err
			}
			for _, c := range d.Children {
				if c.Type == scheme.EntryTable {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
					fullName := dbPath.FullTable(c.Name)
					fmt.Printf("dropping table '%s'\n", fullName)
					err = session.DropTable(ctx, fullName)
					if err != nil {
						cancel()
						return err
					}
					cancel()
				}
			}
			return nil
		},
	}
	command.AddCommand(watcherCmd, dropCmd)

	err = command.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

func tableClient(v *viper.Viper) (*table.Client, error) {
	dialer, err := db.DialerFromViper(v)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	driver, err := dialer.Dial(ctx, v.GetString(db.KeyYdbAddress))
	if err != nil {
		return nil, err
	}
	return &table.Client{Driver: driver}, nil
}
