package watcher

import (
	"context"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

const (
	operationTimeout = time.Minute
	lookahead        = time.Hour * 12
)

var (
	txc = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

type Watcher struct {
	ticker           *time.Ticker
	sessionProvider  table.SessionProvider
	dbPath           schema.DbPath
	expiration       time.Duration
	logger           *zap.Logger
	tableDefinitions map[string]partDefinition
}

func NewWatcher(sp table.SessionProvider, dbPath schema.DbPath, expiration time.Duration, logger *zap.Logger) *Watcher {
	return &Watcher{
		sessionProvider:  sp,
		dbPath:           dbPath,
		expiration:       expiration,
		logger:           logger,
		tableDefinitions: definitions(),
	}
}

func (w *Watcher) Run(interval time.Duration) {
	w.ticker = time.NewTicker(interval)
	go w.watch()
}

func (w *Watcher) watch() {
	go w.createTables()
	go w.dropOldTables()
	for range w.ticker.C {
		w.createTables()
		w.dropOldTables()
	}
}

func (w *Watcher) createTables() {
	t := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()

	for name, definition := range schema.Tables {
		fullName := w.dbPath.FullTable(name)
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.CreateTable(ctx, fullName, definition()...)
		}))
		if err != nil {
			w.logger.Error("create table failed",
				zap.String("name", fullName), zap.Error(err),
			)
		}
	}
	parts := schema.MakePartitionList(t, t.Add(lookahead))
	for _, part := range parts {
		w.logger.Info("creating partition", zap.String("suffix", part.Suffix()))
		if err := w.createTablesForPartition(ctx, part); err != nil {
			break
		}
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			_, _, err := s.Execute(ctx, txc, schema.BuildQuery(w.dbPath, schema.InsertPart), part.QueryParams())
			return err
		}))
		if err != nil {
			w.logger.Error("partition save failed",
				zap.String("suffix", part.Suffix()), zap.Error(err),
			)
			break
		}
	}
}

func (w *Watcher) createTablesForPartition(ctx context.Context, part schema.PartitionKey) error {
	for name, def := range w.tableDefinitions {
		fullName := part.BuildFullTableName(w.dbPath.String(), name)
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.CreateTable(ctx, fullName, def.defFunc(def.count)...)
		}))
		if err != nil {
			w.logger.Error("create table failed",
				zap.String("name", fullName), zap.Error(err),
			)
			return err
		}
	}
	return nil
}

func (w *Watcher) dropOldTables() {
	expireTime := time.Now().Add(-w.expiration)
	w.logger.Info("delete old tables", zap.Time("before", expireTime))
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()

	query := schema.BuildQuery(w.dbPath, schema.QueryParts)
	_ = table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		_, res, err := session.Execute(ctx, txc, query, nil)
		if err != nil {
			w.logger.Error("partition list query failed", zap.Error(err))
			return err
		}
		part := schema.PartitionKey{}
		res.NextSet()
		for res.NextRow() {
			err = part.Scan(res)
			if err != nil {
				w.logger.Error("partition scan failed", zap.Error(err))
				return err
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
		return nil
	}))
}

func (w *Watcher) dropTables(ctx context.Context, session *table.Session, k schema.PartitionKey) error {
	for name := range schema.PartitionTables {
		fullName := k.BuildFullTableName(w.dbPath.String(), name)
		err := session.DropTable(ctx, fullName)
		if err != nil {
			switch {
			// table or path already removed, ignore err
			case ydb.IsOpError(err, ydb.StatusGenericError) && db.IssueContainsMessage(err.(*ydb.OpError).Issues(), "EPathStateNotExist"):
			case ydb.IsOpError(err, ydb.StatusSchemeError) && db.IssueContainsMessage(err.(*ydb.OpError).Issues(), "Path does not exist"):
			default:
				w.logger.Error("drop table failed", zap.String("table", fullName), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (w *Watcher) markPartitionForDelete(ctx context.Context, session *table.Session, k schema.PartitionKey) error {
	k.IsActive = false
	_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.dbPath, schema.UpdatePart), k.QueryParams())
	if err != nil {
		return err
	}
	return nil
}

func (w *Watcher) deletePartitionInfo(ctx context.Context, session *table.Session, k schema.PartitionKey) error {
	_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.dbPath, schema.DeletePart), k.QueryWhereParams())
	if err != nil {
		return err
	}
	return nil
}
