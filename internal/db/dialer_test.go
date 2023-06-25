package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	yc "github.com/ydb-platform/ydb-go-yc"
)

func Test_getCredentialsAndOpts(t *testing.T) {
	type file struct {
		Name string
		Data []byte
	}

	type input struct {
		Envs  [][2]string
		Files []file
	}
	type expect struct {
		Creds     credentials.Credentials
		HaveError bool
		Err       error
	}

	cases := []struct {
		Name       string
		InputData  input
		ExpectData expect
	}{
		{
			Name: "simple: Token",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbToken, "bibaToken"},
				},
			},
			ExpectData: expect{
				Creds:     credentials.NewAccessTokenCredentials("bibaToken"),
				HaveError: false,
			},
		},

		{
			Name: "simple: SaKeyJson",

			InputData: input{
				Envs: [][2]string{
					{
						keyYdbSaKeyJson, `
						{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
						}`,
					},
				},
			},
			ExpectData: expect{
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
				HaveError: false,
			},
		},

		{
			Name: "simple: deprecated key format",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbSaKeyID, "biba_id"},
					{KeyYdbSaId, "biba_sa_id"},
					{KeyYdbSaPrivateKeyFile, "/path/to/biba"},
				},
				Files: []file{
					{
						Name: "/path/to/biba",
						Data: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"),
					},
				},
			},
			ExpectData: expect{
				Creds: func() credentials.Credentials {
					privateKey, _ := parsePrivateKey([]byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"))
					creds, _ := yc.NewClient(
						yc.WithEndpoint(defaultIAMEndpoint),
						yc.WithSystemCertPool(),

						yc.WithKeyID("biba_id"),
						yc.WithIssuer("biba_sa_id"),
						yc.WithPrivateKey(privateKey),
					)
					return creds
				}(),
				HaveError: false,
			},
		},

		{
			Name: "wrong file: deprecated key format",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbSaKeyID, "biba_id"},
					{KeyYdbSaId, "biba_sa_id"},
					{KeyYdbSaPrivateKeyFile, "/wrong/path/to/biba"},
				},
				Files: []file{
					{
						Name: "/path/to/biba",
						Data: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"),
					},
				},
			},
			ExpectData: expect{
				HaveError: true,
				Err:       errCannotOpenFile,
			},
		},

		{
			Name: "bad private key: deprecated key format",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbSaKeyID, "biba_id"},
					{KeyYdbSaId, "biba_sa_id"},
					{KeyYdbSaPrivateKeyFile, "/path/to/biba"},
				},
				Files: []file{
					{
						Name: "/path/to/biba",
						Data: []byte("-----BEGIN RSA PRIVATE KEY-----\nbad key format\n-----END RSA PRIVATE KEY-----\n"),
					},
				},
			},
			ExpectData: expect{
				HaveError: true,
				Err:       errCannotParseKey,
			},
		},

		{
			Name: "conflict: deprecated key format, saKeyJson",

			InputData: input{
				Envs: [][2]string{
					{
						keyYdbSaKeyJson, `
						{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
						}`,
					},
					{KeyYdbSaKeyID, "biba_id"},
					{KeyYdbSaId, "biba_sa_id"},
					{KeyYdbSaPrivateKeyFile, "/path/to/biba"},
				},
				Files: []file{
					{
						Name: "/path/to/biba",
						Data: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"),
					},
				},
			},
			ExpectData: expect{
				HaveError: true,
				Err:       errConflictNewWithDeprecated,
			},
		},

		{
			Name: "no credentials",

			InputData: input{},
			ExpectData: expect{
				HaveError: true,
				Err:       errNoCredentialsAreSpecified,
			},
		},

		{
			Name: "priority check: token, saKeyJson",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbToken, "bibaToken"},
					{
						keyYdbSaKeyJson, `
						{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
						}`,
					},
				},
			},
			ExpectData: expect{
				Creds:     credentials.NewAccessTokenCredentials("bibaToken"),
				HaveError: false,
			},
		},

		{
			Name: "priority check: token, deprecated format key",

			InputData: input{
				Envs: [][2]string{
					{KeyYdbToken, "bibaToken"},
					{
						keyYdbSaKeyJson, `
						{
						  "id": "biba_id",
						  "service_account_id": "biba_sa_id",
						  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"
						}`,
					},
					{KeyYdbSaKeyID, "biba_id"},
					{KeyYdbSaId, "biba_sa_id"},
					{KeyYdbSaPrivateKeyFile, "/path/to/biba"},
				},
				Files: []file{
					{
						Name: "/path/to/biba",
						Data: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBQCIRD8xAgMBAAECBCB4TiUCAwD45wIDAIwnAgJ1ZwICTGsCAjBF\n-----END RSA PRIVATE KEY-----\n"),
					},
				},
			},
			ExpectData: expect{
				Creds:     credentials.NewAccessTokenCredentials("bibaToken"),
				HaveError: false,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			egm := newEnvGetterMock()
			egm.AddString([2]string{KeyIAMEndpoint, defaultIAMEndpoint})
			for _, env := range tc.InputData.Envs {
				egm.AddString(env)
			}

			frm := newFileReaderMock()
			for _, f := range tc.InputData.Files {
				frm.AddFile(f.Name, f.Data)
			}

			creds, _, err := getCredentialsAndOpts(egm, frm)
			if !tc.ExpectData.HaveError {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.ExpectData.Err)
			}

			assert.Equal(t, tc.ExpectData.Creds, creds)
		})
	}
}
