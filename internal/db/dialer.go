package db

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/viper"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"
	"go.uber.org/zap"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

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

type envGetter interface {
	GetString(key string) string
}

type iamStaticKey struct {
	SaId         string
	SaKeyId      string
	SaPrivateKey *rsa.PrivateKey
}

func (isk *iamStaticKey) getFromEnvGetter(e envGetter) error {
	switch {
	case e.GetString(keyYdbSaKeyJson) != "":

		iamStaticKeyFromJson := struct {
			saId            string
			saKeyId         string
			saPrivateKeyRaw string
		}{}
		err := json.Unmarshal([]byte(e.GetString(keyYdbSaKeyJson)), &iamStaticKeyFromJson)
		if err != nil {
			return fmt.Errorf("getFromEnvGetter: %w", err)
		}
		isk.SaId = iamStaticKeyFromJson.saId
		isk.SaKeyId = iamStaticKeyFromJson.saKeyId

		saPrivateKey, err := parsePrivateKey([]byte(iamStaticKeyFromJson.saPrivateKeyRaw))
		if err != nil {
			return fmt.Errorf("getFromEnvGetter: %w", err)
		}
		isk.SaPrivateKey = saPrivateKey

	case e.GetString(KeyYdbSaPrivateKeyFile) != "" &&
		e.GetString(KeyYdbSaId) != "" &&
		e.GetString(KeyYdbSaKeyID) != "":

		isk.SaId = e.GetString(KeyYdbSaId)
		isk.SaKeyId = e.GetString(KeyYdbSaKeyID)

		saPrivateKeyRaw, err := os.ReadFile(e.GetString(KeyYdbSaPrivateKeyFile))
		if err != nil {
			return fmt.Errorf("getFromEnvGetter: %w", err)
		}

		saPrivateKey, err := parsePrivateKey(saPrivateKeyRaw)
		if err != nil {
			return fmt.Errorf("getFromEnvGetter: %w", err)
		}
		isk.SaPrivateKey = saPrivateKey

	default:
		return errors.New("getFromEnvGetter: iam static key not found")
	}

	return nil
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
			yc.WithMetadataCredentials(),
		)
	}
	isk := iamStaticKey{}

	_ = isk.getFromEnvGetter(v)

	return append(
		opts,
		yc.WithAuthClientCredentials(
			yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
			yc.WithKeyID(isk.SaKeyId),
			yc.WithIssuer(isk.SaId),
			yc.WithPrivateKey(isk.SaPrivateKey),
			yc.WithSystemCertPool(),
		),
	)
}

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	return ydb.Open(ctx, dsn, options(v, logger, opts...)...)
}
