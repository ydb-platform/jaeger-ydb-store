package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/viper"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"

	"go.uber.org/zap"
)

const (
	defaultIAMEndpoint = "iam.api.cloud.yandex.net:443"
)

type conflictError struct {
	gotCreds []credentialsType
}

func newConflictError(gotCreds []credentialsType) *conflictError {
	return &conflictError{
		gotCreds: gotCreds,
	}
}

func (c *conflictError) Error() string {
	sort.Slice(c.gotCreds, func(i, j int) bool {
		return c.gotCreds[i] < c.gotCreds[j]
	})
	template := "conflict error: only 1 credentials type type must be specified, got %d: [%s]"
	gotCredsStrBuilder := strings.Builder{}
	for i, v := range c.gotCreds {
		gotCredsStrBuilder.WriteString(v.String())
		if i != len(c.gotCreds)-1 {
			gotCredsStrBuilder.WriteString(", ")
		}
	}

	return fmt.Sprintf(template, len(c.gotCreds), gotCredsStrBuilder.String())
}

var errNotAllSaKeyCredentialsFieldsSpecified = errors.New("not all sa key credentials fields are specified, " +
	"need saId, saKeyId, saPrivateKeyFile")

// isSecureDefault was created to support backward compatibility,
// because previously the type of secure connection was depended on credentials type
func getCredentialsAndSecureType(v *viper.Viper) (creds credentials.Credentials, isSecureDefault bool, err error) {
	var gotCreds []credentialsType

	if v.GetBool(keyYdbAnonymous) {
		gotCreds = append(gotCreds, anonymousCredentials)
		creds = credentials.NewAnonymousCredentials()
		isSecureDefault = false
	}

	if v.GetString(KeyYdbToken) != "" {
		gotCreds = append(gotCreds, accessTokenCredentials)
		creds = credentials.NewAccessTokenCredentials(v.GetString(KeyYdbToken))
		isSecureDefault = false
	}

	if v.GetBool(KeyYdbSaMetaAuth) {
		gotCreds = append(gotCreds, metadataCredentials)
		creds = yc.NewInstanceServiceAccount()
		isSecureDefault = true
	}

	if v.GetString(KeyYdbSaKeyID) != "" ||
		v.GetString(KeyYdbSaId) != "" ||
		v.GetString(KeyYdbSaPrivateKeyFile) != "" {

		if !(v.GetString(KeyYdbSaKeyID) != "" &&
			v.GetString(KeyYdbSaId) != "" &&
			v.GetString(KeyYdbSaPrivateKeyFile) != "") {
			return nil, false, errNotAllSaKeyCredentialsFieldsSpecified
		}

		gotCreds = append(gotCreds, saKeyDeprecatedCredentials)
		creds, err = yc.NewClient(
			yc.WithKeyID(v.GetString(KeyYdbSaKeyID)),
			yc.WithIssuer(v.GetString(KeyYdbSaId)),
			yc.WithPrivateKeyFile(v.GetString(KeyYdbSaPrivateKeyFile)),
			yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
			yc.WithSystemCertPool(),
		)
		if err != nil {
			return nil, false, err
		}
		isSecureDefault = true
	}

	if v.GetString(keyYdbSaKeyJson) != "" {
		gotCreds = append(gotCreds, SaKeyJsonCredentials)
		creds, err = yc.NewClient(
			yc.WithServiceKey(v.GetString(keyYdbSaKeyJson)),
			yc.WithEndpoint(v.GetString(KeyIAMEndpoint)),
			yc.WithSystemCertPool(),
		)
		if err != nil {
			return nil, false, err
		}
		isSecureDefault = true
	}

	if len(gotCreds) != 1 {
		return nil, false, newConflictError(gotCreds)
	}

	return creds, isSecureDefault, nil
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

	if caFile := v.GetString(KeyYdbCAFile); caFile != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caFile))
	}

	creds, isSecureDefault, err := getCredentialsAndSecureType(v)
	if err != nil {
		return nil, err
	}

	opts = append(
		opts,
		ydb.WithCredentials(creds),
		ydb.WithSecure(isSecureDefault),
	)

	return opts, nil
}

func DialFromViper(ctx context.Context, v *viper.Viper, logger *zap.Logger, dsn string, opts ...ydb.Option) (*ydb.Driver, error) {
	gotOptions, err := options(v, logger, opts...)
	if err != nil {
		return nil, err
	}

	return ydb.Open(ctx, dsn, gotOptions...)
}

func UpsertData(ctx context.Context, pool table.Client, tableName string, rows types.Value, retryAttemptTimeout time.Duration) error {
	err := pool.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			opCtx := ctx
			if retryAttemptTimeout > 0 {
				var opCancel context.CancelFunc
				opCtx, opCancel = context.WithTimeout(ctx, retryAttemptTimeout)
				defer opCancel()
			}
			return s.BulkUpsert(opCtx, tableName, rows)
		},
		table.WithIdempotent(),
	)
	return err
}
