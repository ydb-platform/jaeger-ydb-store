package db

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/viper"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-yc"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func DialFromViper(ctx context.Context, v *viper.Viper, opts ...ydb.Option) (ydb.Connection, error) {
	var authCredentials, certFileOpt ydb.Option
	v.SetDefault(KeyIAMEndpoint, defaultIAMEndpoint)
	if v.GetString(KeyYdbToken) != "" {
		authCredentials = ydb.WithAccessTokenCredentials(v.GetString(KeyYdbToken))
	} else {
		if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
			certFileOpt = ydb.WithCertificatesFromFile(caFile)
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

	connOpts := append(opts, authCredentials)
	if certFileOpt != nil {
		connOpts = append(connOpts, certFileOpt, ydb.With(config.WithSecure(true)))
	}
	return ydb.New(ctx, connOpts...)
}

func readFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}

func readPrivateKey(path string) (key *rsa.PrivateKey, err error) {
	p, err := readFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(p)
	if block == nil {
		return nil, fmt.Errorf("invalid pem encoding")
	}
	key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return
	}
	x, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
	if key, _ = x.(*rsa.PrivateKey); key != nil {
		err = nil
	}
	return
}

func readRootCerts(path string) (*x509.CertPool, error) {
	p, err := readFile(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(p); !ok {
		return nil, fmt.Errorf("parse pem error")
	}
	return roots, nil
}

func mustReadRootCerts(path string) *x509.CertPool {
	roots, err := readRootCerts(path)
	if err != nil {
		panic(fmt.Errorf("read root certs error: %v", err))
	}
	return roots
}

func mustReadSystemRootCerts() *x509.CertPool {
	roots, err := x509.SystemCertPool()
	if err != nil {
		panic(fmt.Errorf("read system root certs error: %v", err))
	}
	return roots
}
