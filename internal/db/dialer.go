package db

import (
	"context"

	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"

	"github.com/spf13/viper"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func options(v *viper.Viper, l *zap.Logger, opts ...ydb.Option) []ydb.Option {
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)

	if l != nil {
		x := append(
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
		_ = x
	}

	if v.GetString(KeyYdbToken) != "" {
		return append(
			opts,
			ydb.WithInsecure(),
			ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken)),
		)
	}

	opts = append(opts, ydb.WithSecure(true))

	if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	if v.GetBool(KeyYdbSaMetaAuth) {
		return append(
			opts,
			yc.WithMetadataCredentials(context.Background()),
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

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, opts ...ydb.Option) (ydb.Connection, error) {
	return ydb.New(ctx, options(v, logger, opts...)...)
}
