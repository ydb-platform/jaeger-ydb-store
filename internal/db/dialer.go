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
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)
	var authCredentials ydb.Option
	switch {
	case v.GetString(KeyYdbToken) != "":
		authCredentials = ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken))
	case v.GetBool(KeyYdbSaMetaAuth):
		authCredentials = yc.WithMetadataCredentials(context.Background())
	default:
		authCredentials = yc.WithAuthClientCredentials(
			yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
			yc.WithKeyID(v.GetString(KeyYdbSaKeyID)),
			yc.WithIssuer(v.GetString(KeyYdbSaId)),
			yc.WithPrivateKeyFile(v.GetString(KeyYdbSaPrivateKeyFile)),
			yc.WithSystemCertPool(),
		)
	}

	v.SetDefault(KeyYdbUseTLS, true)
	tlsOptions := []ydb.Option{ydb.With(config.WithSecure(v.GetBool(KeyYdbUseTLS)))}
	if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
		tlsOptions = append(tlsOptions,
			ydb.WithCertificatesFromFile(caFile),
			ydb.With(config.WithSecure(true)), // enforce TLS
		)
	}

	return ydb.New(ctx, append(append(opts, authCredentials), tlsOptions...)...)
}
