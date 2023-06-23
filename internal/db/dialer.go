package db

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
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

func withServiceAccountKeyJson(data string) ([]yc.ClientOption, error) {
	type keyFile struct {
		ID               string `json:"id"`
		ServiceAccountID string `json:"service_account_id"`
		PrivateKey       string `json:"private_key"`
	}
	var info keyFile
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, fmt.Errorf("withServiceAccountKeyJson: %w", err)
	}
	privateKey, err := parsePrivateKey([]byte(info.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("withServiceAccountKeyJson: %w", err)
	}
	return []yc.ClientOption{
		yc.WithIssuer(info.ServiceAccountID),
		yc.WithKeyID(info.ID),
		yc.WithPrivateKey(privateKey),
	}, nil
}

func getCredentialsAndOpts(eg EnvGetter) (creds credentials.Credentials, opts []ydb.Option, err error) {

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
		keyClientOption, err := withServiceAccountKeyJson(eg.GetString(keyYdbSaKeyJson))
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		creds, err = yc.NewClient(
			append(
				keyClientOption,
				yc.WithEndpoint(KeyIAMEndpoint),
				yc.WithSystemCertPool(),
			)...,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		opts = append(opts, ydb.WithSecure(true))

	case eg.GetString(KeyYdbSaKeyID) != "" &&
		eg.GetString(KeyYdbSaId) != "" &&
		eg.GetString(KeyYdbSaPrivateKeyFile) != "":

		creds, err = yc.NewClient(
			yc.WithEndpoint(KeyIAMEndpoint),
			yc.WithSystemCertPool(),

			yc.WithIssuer(eg.GetString(KeyYdbSaId)),
			yc.WithKeyID(eg.GetString(KeyYdbSaKeyID)),
			yc.WithPrivateKeyFile(eg.GetString(KeyYdbSaPrivateKeyFile)),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		opts = append(opts, ydb.WithSecure(true))

	default:
		creds = credentials.NewAnonymousCredentials()
		opts = append(opts, ydb.WithInsecure())
	}

	return creds, opts, err
}

func options(v *viper.Viper, l *zap.Logger, opts ...ydb.Option) ([]ydb.Option, error) {
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

	creds, extraOps, err := getCredentialsAndOpts(v)
	if err != nil {
		return nil, fmt.Errorf("options: %w", err)
	}

	opts = append(opts, ydb.WithCredentials(creds))
	opts = append(opts, extraOps...)

	return opts, nil

}

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	optsFromOptions, err := options(v, logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("DialFromViper: %w", err)
	}
	return ydb.Open(ctx, dsn, optsFromOptions...)
}
