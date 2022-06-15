package watcher

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

const (
	operationTimeout = time.Minute
)

var txc = table.DefaultTxControl()

type Options struct {
	Expiration time.Duration
	Lookahead  time.Duration
	DBPath     schema.DbPath
}

type Watcher struct {
	sessionProvider table.Client
	opts            Options
	logger          *zap.Logger

	ticker           *time.Ticker
	tableDefinitions map[string]partDefinition
	knownTables      *lru.Cache
}

func NewWatcher(opts Options, sp table.Client, logger *zap.Logger) *Watcher {
	return &Watcher{
		sessionProvider: sp,
		opts:            opts,
		logger:          logger,

		tableDefinitions: definitions(),
		knownTables:      mustNewLRU(500),
	}
}

func (w *Watcher) Run(interval time.Duration) {
	w.ticker = time.NewTicker(interval)
	go func() {
		w.once()
		for range w.ticker.C {
			w.once()
		}
	}()
}

func (w *Watcher) once() {
	err := w.createTables()
	if err != nil {
		w.logger.Error("create tables failed",
			zap.Error(err),
		)
		return
	}
	w.dropOldTables()
}

func (w *Watcher) createTables() error {
	t := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()

	for name, definition := range schema.Tables {
		fullName := w.opts.DBPath.FullTable(name)
		if w.tableKnown(ctx, fullName) {
			// We already created this table, skip
			continue
		}
		err := w.sessionProvider.Do(ctx, func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, fullName, definition()...)
		})
		if err != nil {
			w.logger.Error("create table failed",
				zap.String("name", fullName), zap.Error(err),
			)
			return err
		}
		// save knowledge about table for later
		w.knownTables.Add(fullName, struct{}{})
	}
	parts := schema.MakePartitionList(t, t.Add(w.opts.Lookahead))
	for _, part := range parts {
		w.logger.Info("creating partition", zap.String("suffix", part.Suffix()))
		if err := w.createTablesForPartition(ctx, part); err != nil {
			return err
		}
		err := w.sessionProvider.Do(ctx, func(ctx context.Context, session table.Session) error {
			_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.opts.DBPath, schema.InsertPart), part.QueryParams())
			return err
		})
		if err != nil {
			w.logger.Error("partition save failed",
				zap.String("suffix", part.Suffix()), zap.Error(err),
			)
			return err
		}
	}
	return nil
}

func (w *Watcher) createTablesForPartition(ctx context.Context, part schema.PartitionKey) error {
	for name, def := range w.tableDefinitions {
		fullName := part.BuildFullTableName(w.opts.DBPath.String(), name)
		if w.tableKnown(ctx, fullName) {
			// We already created this table, skip
			continue
		}
		err := w.sessionProvider.Do(ctx, func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, fullName, def.defFunc(def.count)...)
		})
		if err != nil {
			w.logger.Error("create table failed",
				zap.String("name", fullName), zap.Error(err),
			)
			return err
		}
		// save knowledge about table for later
		w.knownTables.Add(fullName, struct{}{})
	}
	return nil
}

func (w *Watcher) dropOldTables() {
	expireTime := time.Now().Add(-w.opts.Expiration)
	w.logger.Info("delete old tables", zap.Time("before", expireTime))
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()

	query := schema.BuildQuery(w.opts.DBPath, schema.QueryParts)
	_ = w.sessionProvider.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(ctx, txc, query, nil)
		if err != nil {
			w.logger.Error("partition list query failed", zap.Error(err))
			return err
		}
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				part := schema.PartitionKey{}
				err = res.ScanWithDefaults(&part.Date, &part.Num, &part.IsActive)
				if err != nil {
					w.logger.Error("partition scan failed", zap.Error(err))
					return fmt.Errorf("part scan err: %w", err)
				}
				_, t := part.TimeSpan()
				if expireTime.Sub(t) > 0 {
					if part.IsActive {
						err := w.markPartitionForDelete(ctx, session, part)
						if err != nil {
							w.logger.Error("update partition failed", zap.String("suffix", part.Suffix()), zap.Error(err))
						}
					} else {
						w.logger.Info("delete partition", zap.String("suffix", part.Suffix()))
						if err := w.dropTables(ctx, session, part); err != nil {
							continue
						}
						err = w.deletePartitionInfo(ctx, session, part)
						if err != nil {
							w.logger.Error("delete partition failed", zap.String("suffix", part.Suffix()), zap.Error(err))
						}
					}
				}
			}
		}
		return nil
	})
}

func (w *Watcher) dropTables(ctx context.Context, session table.Session, k schema.PartitionKey) error {
	for name := range schema.PartitionTables {
		fullName := k.BuildFullTableName(w.opts.DBPath.String(), name)
		err := session.DropTable(ctx, fullName)
		if err != nil {
			opErr := ydb.OperationError(err)
			switch {
			// table or path already removed, ignore err
			case opErr != nil && db.IssueContainsMessage(err, "EPathStateNotExist"):
			case ydb.IsOperationErrorSchemeError(err) && db.IssueContainsMessage(err, "Path does not exist"):
			default:
				w.logger.Error("drop table failed", zap.String("table", fullName), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (w *Watcher) markPartitionForDelete(ctx context.Context, session table.Session, k schema.PartitionKey) error {
	k.IsActive = false
	_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.opts.DBPath, schema.UpdatePart), k.QueryParams())
	if err != nil {
		return err
	}
	return nil
}

func (w *Watcher) deletePartitionInfo(ctx context.Context, session table.Session, k schema.PartitionKey) error {
	_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.opts.DBPath, schema.DeletePart), k.QueryWhereParams())
	if err != nil {
		return err
	}
	return nil
}

func (w *Watcher) tableKnown(ctx context.Context, fullName string) bool {
	if _, ok := w.knownTables.Get(fullName); ok {
		return true
	}
	err := w.sessionProvider.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, err := session.DescribeTable(ctx, fullName)
		return err
	})
	if err != nil {
		return false
	}
	w.knownTables.Add(fullName, struct{}{})
	return true
}
