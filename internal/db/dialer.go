package db

import (
	"context"
	"time"

	"github.com/spf13/viper"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"
	"go.uber.org/zap"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func options(v *viper.Viper, l *zap.Logger, opts ...ydb.Option) []ydb.Option {
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)

	if l != nil {
		opts = append(
			opts,
			ydbZap.WithTraces(
				l,
				trace.MatchDetails(
					viper.GetString(KeyYdbLogScope),
					trace.WithDefaultDetails(
						trace.DiscoveryEvents,
					),
				),
			),
		)
	}

	if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	if v.GetString(KeyYdbToken) != "" {
		return append(
			opts,
			ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken)),
		)
	}

	opts = append(opts, ydb.WithSecure(true))
	if v.GetBool(KeyYdbSaMetaAuth) {
		return append(
			opts,
			yc.WithMetadataCredentials(),
		)
	}

	return append(
		opts,
		yc.WithAuthClientCredentials(
			yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
			yc.WithKeyID(v.GetString(KeyYdbSaKeyID)),
			yc.WithIssuer(v.GetString(KeyYdbSaId)),
			yc.WithPrivateKeyFile(v.GetString(KeyYdbSaPrivateKeyFile)),
			yc.WithSystemCertPool(),
		),
	)
}

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	return ydb.Open(ctx, dsn, options(v, logger, opts...)...)
}

func UpsertData(ctx context.Context, pool table.Client, tableName string, rows types.Value, retryAttemptTimeout time.Duration) error {
	err := pool.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			opCtx := ctx
			if retryAttemptTimeout > 0 {
				var opCancel context.CancelFunc
				opCtx, opCancel = context.WithTimeout(ctx, retryAttemptTimeout)
				defer opCancel()
			}
			return s.BulkUpsert(opCtx, tableName, rows)
		},
		table.WithIdempotent(),
	)
	return err
}
