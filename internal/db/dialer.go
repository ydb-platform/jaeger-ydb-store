package db

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func options(v *viper.Viper, opts ...ydb.Option) []ydb.Option {
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)

	// temporary solution before merge with feature/sa-key-json
	if v.GetBool(KeyYdbAnonymous) == true {
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

func ConnectToYDB(ctx context.Context, v *viper.Viper, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	conn, err := ydb.Open(ctx, dsn, options(v, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("ConnectToYDB(): %w", err)
	}

	err = conn.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			return s.KeepAlive(ctx)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("ConnectToYDB(): %w", err)
	}

	return conn, nil
}
