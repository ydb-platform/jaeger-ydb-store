package db

import (
	"context"

	"github.com/spf13/viper"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-yc"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func DialFromViper(ctx context.Context, v *viper.Viper, opts ...ydb.Option) (ydb.Connection, error) {
	var authCredentials ydb.Option
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)
	connOpts := opts
	if v.GetString(KeyYdbToken) != "" {
		authCredentials = ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken))
	} else {
		connOpts = append(connOpts, ydb.With(config.WithSecure(true)))
		if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
			connOpts = append(connOpts, ydb.WithCertificatesFromFile(caFile))
		}

		if v.GetBool(KeyYdbSaMetaAuth) {
			authCredentials = yc.WithMetadataCredentials(context.Background())
		} else {
			authCredentials = yc.WithAuthClientCredentials(
				yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
				yc.WithKeyID(v.GetString(KeyYdbSaKeyID)),
				yc.WithIssuer(v.GetString(KeyYdbSaId)),
				yc.WithPrivateKeyFile(v.GetString(KeyYdbSaPrivateKeyFile)),
				yc.WithSystemCertPool(),
			)
		}
	}
	connOpts = append(connOpts, authCredentials)

	return ydb.New(ctx, connOpts...)
}
