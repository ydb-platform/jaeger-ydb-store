package db

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
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
	"os"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

var (
	errCannotParseKey            = errors.New("cannot parse key")
	errCannotOpenFile            = errors.New("cannot open file")
	errConflictNewWithDeprecated = errors.New("new format key conflicts with deprecated")
	errNoCredentialsAreSpecified = errors.New("no credentials are specified")
)

type EnvGetter interface {
	GetString(key string) string
	GetBool(key string) bool
}

type FileReader interface {
	ReadFile(name string) ([]byte, error)
}

type osFileReader struct{}

func (ofr *osFileReader) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func parsePrivateKey(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("parsePrivateKey: %w", errCannotParseKey)
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, err
	}

	x, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsePrivateKey: %w", errCannotParseKey)
	}
	if key, ok := x.(*rsa.PrivateKey); ok {
		return key, nil
	}
	return nil, fmt.Errorf("parsePrivateKey: %w", errCannotParseKey)
}

func readPrivateKeyFromFile(path string, fr FileReader) (*rsa.PrivateKey, error) {
	data, err := fr.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("readPrivateKeyFromFile %w", errCannotOpenFile)
	}

	privateKey, err := parsePrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("readPrivateKeyFromFile %w", err)
	}
	return privateKey, nil
}

func getCredentialsAndOpts(eg EnvGetter, fr FileReader) (creds credentials.Credentials, opts []ydb.Option, err error) {

	if caFile := eg.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	switch {
	case eg.GetString(KeyYdbToken) != "":
		creds = credentials.NewAccessTokenCredentials(eg.GetString(KeyYdbToken))
		opts = append(opts, ydb.WithInsecure())

	case eg.GetBool(KeyYdbSaMetaAuth) == true:
		creds = yc.NewInstanceServiceAccount()
		opts = append(opts, ydb.WithSecure(true))

	case eg.GetString(keyYdbSaKeyJson) != "":

		if eg.GetString(KeyYdbSaKeyID) != "" ||
			eg.GetString(KeyYdbSaId) != "" ||
			eg.GetString(KeyYdbSaPrivateKeyFile) != "" {

			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", errConflictNewWithDeprecated)
		}

		keyClientOption := yc.WithServiceKey(eg.GetString(keyYdbSaKeyJson))

		creds, err = yc.NewClient(
			keyClientOption,
			yc.WithEndpoint(eg.GetString(KeyIAMEndpoint)),
			yc.WithSystemCertPool(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		opts = append(opts, ydb.WithSecure(true))

	case eg.GetString(KeyYdbSaKeyID) != "" ||
		eg.GetString(KeyYdbSaId) != "" ||
		eg.GetString(KeyYdbSaPrivateKeyFile) != "":

		privateKey, err := readPrivateKeyFromFile(eg.GetString(KeyYdbSaPrivateKeyFile), fr)
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		creds, err = yc.NewClient(
			yc.WithEndpoint(eg.GetString(KeyIAMEndpoint)),
			yc.WithSystemCertPool(),

			yc.WithIssuer(eg.GetString(KeyYdbSaId)),
			yc.WithKeyID(eg.GetString(KeyYdbSaKeyID)),
			yc.WithPrivateKey(privateKey),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", err)
		}
		opts = append(opts, ydb.WithSecure(true))

	default:
		return nil, nil, fmt.Errorf("getCredentialsAndOpts: %w", errNoCredentialsAreSpecified)
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

	creds, extraOps, err := getCredentialsAndOpts(v, &osFileReader{})
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
