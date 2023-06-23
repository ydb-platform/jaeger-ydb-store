package db

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/spf13/viper"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"
	"go.uber.org/zap"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

type EnvGetter interface {
	SetDefault(key string, value interface{})
	GetString(key string) string
}

func parsePrivateKey(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("key cannot be parsed")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, err
	}

	x, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	if key, ok := x.(*rsa.PrivateKey); ok {
		return key, nil
	}
	return nil, errors.New("key cannot be parsed")
}

func withServiceAccountKeyJson(data string) []yc.ClientOption {
	type keyFile struct {
		ID               string `json:"id"`
		ServiceAccountID string `json:"service_account_id"`
		PrivateKey       string `json:"private_key"`
	}
	var info keyFile
	_ = json.Unmarshal([]byte(data), &info)
	privateKey, _ := parsePrivateKey([]byte(info.PrivateKey))

	return []yc.ClientOption{
		yc.WithIssuer(info.ServiceAccountID),
		yc.WithKeyID(info.ID),
		yc.WithPrivateKey(privateKey),
	}
}

func getCredentialsAndOpts(eg EnvGetter) (creds credentials.Credentials, opts []ydb.Option) {

	if caFile := eg.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	switch {
	case eg.GetString(KeyYdbToken) != "":
		creds = credentials.NewAccessTokenCredentials(eg.GetString(KeyYdbToken))
		opts = append(opts, ydb.WithInsecure())

	case eg.GetString(KeyYdbSaMetaAuth) != "":
		creds = yc.NewInstanceServiceAccount()
		opts = []ydb.Option{ydb.WithSecure(true)}

	case eg.GetString(keyYdbSaKeyJson) != "":
		creds, _ = yc.NewClient(
			append(
				withServiceAccountKeyJson(eg.GetString(keyYdbSaKeyJson)),
				yc.WithEndpoint(KeyIAMEndpoint),
				yc.WithSystemCertPool(),
			)...,
		)
		opts = append(opts, ydb.WithSecure(true))

	case eg.GetString(KeyYdbSaKeyID) != "" &&
		eg.GetString(KeyYdbSaId) != "" &&
		eg.GetString(KeyYdbSaPrivateKeyFile) != "":

		creds, _ = yc.NewClient(
			yc.WithEndpoint(KeyIAMEndpoint),
			yc.WithSystemCertPool(),

			yc.WithIssuer(eg.GetString(KeyYdbSaId)),
			yc.WithKeyID(eg.GetString(KeyYdbSaKeyID)),
			yc.WithPrivateKeyFile(eg.GetString(KeyYdbSaPrivateKeyFile)),
		)
		opts = append(opts, ydb.WithSecure(true))

	default:
		creds = credentials.NewAnonymousCredentials()
		opts = append(opts, ydb.WithInsecure())
	}

	return creds, opts
}

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

	creds, extraOps := getCredentialsAndOpts(v)
	opts = append(opts, ydb.WithCredentials(creds))
	opts = append(opts, extraOps...)

	return opts

}

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	return ydb.Open(ctx, dsn, options(v, logger, opts...)...)
}
