package main

import (
	"context"
	"fmt"
	"github.com/YandexClassifieds/jaeger-ydb-store/cmd/schema/watcher"
	"github.com/YandexClassifieds/jaeger-ydb-store/internal/db"
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yandex-cloud/ydb-go-sdk/scheme"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"log"
	"time"
)

const (
	keyYdbAddress = "ydb.address"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.SetDefault("watcher_interval", time.Minute*5)
	viper.SetDefault("watcher_age", time.Hour*24)
	viper.SetDefault("parts_traces", 32)
	viper.SetDefault("parts_idx_tag", 32)
	viper.SetDefault("parts_idx_duration", 32)
	viper.SetDefault("parts_idx_service_name", 32)
	viper.SetDefault("parts_idx_service_op", 32)
	viper.AutomaticEnv()
}

func main() {
	command := &cobra.Command{
		Use: "jaeger-ydb-schema",
	}
	command.PersistentFlags().String("address", "", "ydb host:port (env: YDB_ADDRESS)")
	command.PersistentFlags().String("path", "", "ydb path (env: YDB_PATH)")
	command.PersistentFlags().String("folder", "", "ydb folder (env: YDB_FOLDER)")
	command.PersistentFlags().String("token", "", "ydb oauth token (env: YDB_TOKEN)")
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
			shutdown := make(chan os.Signal)
			signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
			dbPath := schema.DbPath{
				Path:   viper.GetString("ydb_path"),
				Folder: viper.GetString("ydb_folder"),
			}
			tc, err := tableClient(viper.GetViper())
			if err != nil {
				return fmt.Errorf("failed to create table client: %w", err)
			}
			pool := &table.SessionPool{
				Builder: tc,
			}

			logger.Info("starting watcher")
			w := watcher.NewWatcher(pool, dbPath, viper.GetDuration("watcher_age"), logger)
			w.Run(viper.GetDuration("watcher_interval"))
			<-shutdown
			logger.Info("stopping watcher")
			return nil
		},
	}
	dropCmd := &cobra.Command{
		Use: "drop-tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			dbPath := schema.DbPath{
				Path:   viper.GetString("ydb_path"),
				Folder: viper.GetString("ydb_folder"),
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
	driver, err := dialer.Dial(ctx, v.GetString(keyYdbAddress))
	if err != nil {
		return nil, err
	}
	return &table.Client{Driver: driver}, nil
}
