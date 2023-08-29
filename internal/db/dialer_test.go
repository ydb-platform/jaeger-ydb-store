package db

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	yc "github.com/ydb-platform/ydb-go-yc"
)

func Test_getCredentialsAndSecureType(t *testing.T) {
	f, err := os.CreateTemp("", "tmpfile-")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	})
	_, err = f.WriteString("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n")
	require.NoError(t, err)

	type input struct {
		Env map[string]string
	}
	type expect struct {
		Creds     credentials.Credentials
		IsSecure  bool
		HaveError bool
		Err       error
	}

	tests := []struct {
		Name       string
		inputData  input
		expectData expect
	}{
		{
			Name: "NoCredsSpecified",
			expectData: expect{
				HaveError: true,
				Err:       &conflictError{gotCreds: []credentialsType{}},
			},
		},
		{
			Name: "AnonymousSimple",
			inputData: input{
				Env: map[string]string{
					keyYdbAnonymous: "true",
				},
			},
			expectData: expect{
				Creds:     credentials.NewAnonymousCredentials(),
				IsSecure:  false,
				HaveError: false,
			},
		},
		{
			Name: "AccessTokenSimple",
			inputData: input{
				Env: map[string]string{
					KeyYdbToken: "tokenexample",
				},
			},
			expectData: expect{
				Creds: credentials.NewAccessTokenCredentials(
					"tokenexample",
				),
				IsSecure:  false,
				HaveError: false,
			},
		},
		{
			Name: "SaKeyDeprecatedSimple",
			inputData: input{
				Env: map[string]string{
					KeyYdbSaId:             "biba-id",
					KeyYdbSaKeyID:          "biba-key-id",
					KeyYdbSaPrivateKeyFile: f.Name(),
				},
			},
			expectData: expect{
				Creds: func() credentials.Credentials {
					creds, err := yc.NewClient(
						yc.WithEndpoint(defaultIAMEndpoint),
						yc.WithSystemCertPool(),

						yc.WithKeyID("biba-id"),
						yc.WithIssuer("biba-sa-id"),
						yc.WithPrivateKeyFile(f.Name()),
					)
					require.NoError(t, err)
					return creds
				}(),
				IsSecure:  true,
				HaveError: false,
			},
		},
		{
			Name: "SaKeyDeprecatedNotAll",
			inputData: input{
				Env: map[string]string{
					KeyYdbSaId:             "biba-id",
					KeyYdbSaPrivateKeyFile: f.Name(),
				},
			},
			expectData: expect{
				Creds: func() credentials.Credentials {
					creds, err := yc.NewClient(
						yc.WithEndpoint(defaultIAMEndpoint),
						yc.WithSystemCertPool(),

						yc.WithKeyID("biba-id"),
						yc.WithIssuer("biba-sa-id"),
						yc.WithPrivateKeyFile(f.Name()),
					)
					require.NoError(t, err)
					return creds
				}(),
				IsSecure:  true,
				HaveError: true,
				Err:       errNotAllSaKeyCredentialsFieldsSpecified,
			},
		},
		{
			Name: "SaKeyJsonSimple",
			inputData: input{
				Env: map[string]string{
					keyYdbSaKeyJson: `
					{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
					}`,
				},
			},
			expectData: expect{
				Creds: func() credentials.Credentials {
					keyClientOption := yc.WithServiceKey(`
						{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
						}`)
					creds, err := yc.NewClient(
						keyClientOption,
						yc.WithEndpoint(defaultIAMEndpoint),
						yc.WithSystemCertPool(),
					)
					require.NoError(t, err)
					return creds
				}(),
				IsSecure:  true,
				HaveError: false,
			},
		},
		{
			Name: "Conflict",
			inputData: input{
				Env: map[string]string{
					keyYdbAnonymous: "true",
					KeyYdbToken:     "tokenexample",
				},
			},
			expectData: expect{
				HaveError: true,
				Err: &conflictError{
					gotCreds: []credentialsType{anonymousCredentials, accessTokenCredentials},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			v := viper.New()
			v.Set(KeyIAMEndpoint, defaultIAMEndpoint)
			for key, value := range tt.inputData.Env {
				v.Set(key, value)
			}

			_, isSecure, err := getCredentialsAndSecureType(v)
			if tt.expectData.HaveError {
				require.Equal(t, tt.expectData.Err.Error(), err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectData.IsSecure, isSecure)
			}
		})
	}
}
