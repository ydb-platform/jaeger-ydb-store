package watcher

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

const (
	operationTimeout = time.Minute
)

var (
	txc = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

type Options struct {
	Expiration time.Duration
	Lookahead  time.Duration
	DBPath     schema.DbPath
}

type Watcher struct {
	sessionProvider table.SessionProvider
	opts            Options
	logger          *zap.Logger

	ticker           *time.Ticker
	tableDefinitions map[string]partDefinition
	knownTables      *lru.Cache
}

func NewWatcher(opts Options, sp table.SessionProvider, logger *zap.Logger) *Watcher {
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
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.CreateTable(ctx, fullName, definition()...)
		}))
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
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			_, _, err := s.Execute(ctx, txc, schema.BuildQuery(w.opts.DBPath, schema.InsertPart), part.QueryParams())
			return err
		}))
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
		err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.CreateTable(ctx, fullName, def.defFunc(def.count)...)
		}))
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
		fullName := k.BuildFullTableName(w.opts.DBPath.String(), name)
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
	_, _, err := session.Execute(ctx, txc, schema.BuildQuery(w.opts.DBPath, schema.UpdatePart), k.QueryParams())
	if err != nil {
		return err
	}
	return nil
}

func (w *Watcher) deletePartitionInfo(ctx context.Context, session *table.Session, k schema.PartitionKey) error {
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
	err := table.Retry(ctx, w.sessionProvider, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		_, err := session.DescribeTable(ctx, fullName)
		return err
	}))
	if err != nil {
		return false
	}
	w.knownTables.Add(fullName, struct{}{})
	return true
}
