package db

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/spf13/viper"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/auth/iam"
)

const (
	keyYdbAddress          = "ydb.address"
	keyYdbPath             = "ydb.path"
	keyYdbFolder           = "ydb.folder"
	keyYdbToken            = "ydb.token"
	keyYdbSaPrivateKeyFile = "ydb.sa.private-key-file"
	keyYdbSaId             = "ydb.sa.id"
	keyYdbSaKeyId          = "ydb.sa.key-id"
	keyYdbMetadataAuth     = "ydb.sa.metadata-auth"
	keyYdbCAFile           = "ydb.ca-file"
	keyIAMEndpoint         = "iam.endpoint"

	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

func DialerFromViper(v *viper.Viper) (*ydb.Dialer, error) {
	var authCredentials ydb.Credentials
	var tlsConfig *tls.Config
	v.SetDefault(keyIAMEndpoint, defaultIAMEndpoint)
	if v.GetString(keyYdbToken) != "" {
		authCredentials = ydb.AuthTokenCredentials{AuthToken: v.GetString(keyYdbToken)}
	} else {
		var certPool *x509.CertPool
		if caFile := v.GetString(keyYdbCAFile); caFile != "" {
			certPool = mustReadRootCerts(caFile)
		} else {
			certPool = mustReadSystemRootCerts()
		}
		tlsConfig = &tls.Config{
			RootCAs: certPool,
		}
		var err error
		if v.GetBool(keyYdbMetadataAuth) {
			sdk, err := ycsdk.Build(context.Background(), ycsdk.Config{
				Credentials: ycsdk.InstanceServiceAccount(),
				TLSConfig:   tlsConfig,
			})
			if err != nil {
				return nil, err
			}
			authCredentials = &cachedSDKCredentials{
				mux:           &sync.RWMutex{},
				renewOverhead: 5 * time.Second,
				sdk:           sdk,
			}
		} else {
			authCredentials, err = iam.NewClient(
				iam.WithEndpoint(v.GetString(keyIAMEndpoint)),
				iam.WithKeyID(v.GetString(keyYdbSaKeyId)),
				iam.WithIssuer(v.GetString(keyYdbSaId)),
				iam.WithPrivateKeyFile(v.GetString(keyYdbSaPrivateKeyFile)),
				iam.WithSystemCertPool(),
			)
			if err != nil {
				return nil, err
			}
		}
	}
	return &ydb.Dialer{
		TLSConfig: tlsConfig,
		DriverConfig: &ydb.DriverConfig{
			Database:        v.GetString(keyYdbPath),
			Credentials:     authCredentials,
			BalancingMethod: ydb.BalancingP2C,
			BalancingConfig: &ydb.P2CConfig{
				PreferLocal:     true,
				OpTimeThreshold: time.Second,
			},
			DiscoveryInterval: time.Minute,
		},
	}, nil
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
