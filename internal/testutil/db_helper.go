package testutil

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

var (
	db struct {
		once sync.Once
		pool table.Client
		done bool
	}

	defaultTXC = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

func initDb(tb testing.TB) {
	db.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		dbPath := schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")}
		require.NotEmpty(tb, os.Getenv("YDB_ADDRESS"))
		conn, err := ydb.New(ctx,
			ydb.WithConnectParams(ydb.EndpointDatabase(os.Getenv("YDB_ADDRESS"), dbPath.Path, false)),
			ydb.WithSessionPoolSizeLimit(10),
			ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
		)
		if err != nil {
			tb.Fatalf("ydb connect failed: %v", err)
		}
		db.pool = conn.Table()

		err = conn.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
			return CreateTables(ctx, dbPath, session)
		})
		if err != nil {
			tb.Fatalf("failed to create static tables: %v", err)
		}
		err = conn.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
			_, _, err := session.Execute(ctx, defaultTXC, schema.BuildQuery(dbPath, schema.DeleteAllParts), nil)
			return err
		})
		if err != nil {
			tb.Fatalf("failed to clean part table: %v", err)
		}

		err = CreatePartitionTables(ctx, conn.Table(), partRange(time.Now(), time.Now().Add(time.Hour*2))...)
		if err != nil {
			tb.Fatalf("failed to create partition tables: %v", err)
		}

		db.done = true
	})
	if !db.done {
		tb.Fatal("initDb failed")
	}
}

func YdbSessionPool(tb testing.TB) table.Client {
	initDb(tb)
	return db.pool
}

func partRange(start, stop time.Time) []schema.PartitionKey {
	result := make([]schema.PartitionKey, 0)
	diff := stop.Sub(start)
	if diff < 0 {
		panic("stop < start")
	}
	numParts := int(math.Floor(diff.Hours())) + 1
	for i := 0; i < numParts; i++ {
		result = append(result, schema.PartitionFromTime(start.Add(time.Hour*time.Duration(i))))
	}
	return result
}

func CreateTables(ctx context.Context, dbPath schema.DbPath, session table.Session) error {
	for name, definition := range schema.Tables {
		fullPath := dbPath.FullTable(name)
		if err := session.CreateTable(ctx, fullPath, definition()...); err != nil {
			return err
		}
	}
	return nil
}

func CreatePartitionTables(ctx context.Context, tc table.Client, parts ...schema.PartitionKey) error {
	var err error
	dbPath := schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")}

	for _, part := range parts {
		err = tc.Do(ctx, func(ctx context.Context, session table.Session) error {
			_, _, err = session.Execute(ctx, defaultTXC, schema.BuildQuery(dbPath, schema.InsertPart), part.QueryParams())
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to insert part '%+v': %v", part, err)
		}
		for name, tableOptions := range schema.PartitionTables {
			fullPath := part.BuildFullTableName(dbPath.String(), name)
			tbl := dbPath.Table(name)
			tc.Do(ctx, func(ctx context.Context, session table.Session) error {
				return session.CreateTable(ctx, fullPath, tableOptions(3)...)
			})
			if err != nil {
				return fmt.Errorf("failed to create table '%s': %v", fullPath, err)
			}

			err = tc.Do(ctx, func(ctx context.Context, session table.Session) error {
				_, _, err = session.Execute(ctx, defaultTXC, fmt.Sprintf("DELETE FROM `%s_%s`", tbl, part.Suffix()), table.NewQueryParameters())
				return err
			})
			if err != nil {
				return fmt.Errorf("failed to clean table '%s': %v", fullPath, err)
			}
		}
	}
	return nil
}
