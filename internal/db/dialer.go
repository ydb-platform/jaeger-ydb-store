package db

import (
	"context"

	"github.com/spf13/viper"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc"
	"go.uber.org/zap"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func options(v *viper.Viper, _ *zap.Logger, opts ...ydb.Option) []ydb.Option {
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)

	// temporary solution before merge with feature/sa-key-json
	if v.GetBool("YDB_ANONYMOUS") == true {
		return append(
			opts,
			ydb.WithInsecure(),
			ydb.WithAnonymousCredentials(),
		)
	}

	if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	opts = append(opts, ydb.WithSecure(true))
	if v.GetString(KeyYdbToken) != "" {
		return append(
			opts,
			ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken)),
		)
	}

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
