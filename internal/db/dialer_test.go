package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_iamStaticKey_getFromEnvGetter(t *testing.T) {
	type input struct {
		Envs     [][2]string
		FileName string
		FileData []byte
	}
	type expect struct {
		SaId            string
		SaKeyId         string
		SaPrivateKeyRaw []byte
		HaveError       bool
	}

	cases := []struct {
		name string
		i    input
		e    expect
	}{
		{
			"simple deprecated format",
			input{
				Envs: [][2]string{
					{KeyYdbSaId, "biba_id"},
					{KeyYdbSaKeyID, "biba_key_id"},
					{KeyYdbSaPrivateKeyFile, "ydb.key"},
				},
				FileName: "ydb.key",
				FileData: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"),
			},
			expect{
				SaId:            "biba_id",
				SaKeyId:         "biba_key_id",
				SaPrivateKeyRaw: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"),
				HaveError:       false,
			},
		}, {
			"wrong deprecated format",
			input{
				Envs: [][2]string{
					{KeyYdbSaId, "biba_id"},
					{KeyYdbSaPrivateKeyFile, "ydb.key"},
				},
			},
			expect{
				HaveError: true,
			},
		}, {
			"simple json format",
			input{
				Envs: [][2]string{
					{keyYdbSaKeyJson, `{"id": "biba_id",
										"service_account_id": "biba_key_id",
										"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"}`},
				},
			},
			expect{
				SaId:            "biba_id",
				SaKeyId:         "biba_key_id",
				SaPrivateKeyRaw: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"),
				HaveError:       false,
			},
		}, {
			"wrong json format",
			input{
				Envs: [][2]string{
					{keyYdbSaKeyJson, `{"id": "biba_id",
										"service_account_id_wrong_field": "biba_key_id",
										"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"}`},
				},
			},
			expect{
				HaveError: true,
			},
		}, {
			"conflict",
			input{
				Envs: [][2]string{
					{KeyYdbSaId, "deprecated_biba_id"},
					{KeyYdbSaKeyID, "deprecated_biba_key_id"},
					{KeyYdbSaPrivateKeyFile, "ydb.key"},

					{keyYdbSaKeyJson, `{"id": "json_biba_id",
										"service_account_id": "json_biba_key_id",
										"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"}`},
				},
				FileName: "ydb.key",
				FileData: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"),
			},
			expect{
				SaId:            "json_biba_id",
				SaKeyId:         "json_biba_key_id",
				SaPrivateKeyRaw: []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n"),
				HaveError:       false,
			},
		}, {
			"neither deprecated not json",
			input{},
			expect{
				HaveError: true,
			},
		}, {
			"bad private key",
			input{
				Envs: [][2]string{
					{KeyYdbSaId, "biba_id"},
					{KeyYdbSaKeyID, "biba_key_id"},
					{KeyYdbSaPrivateKeyFile, "ydb.key"},
				},
				FileName: "ydb.key",
				FileData: []byte("wrong key format"),
			},
			expect{
				HaveError: true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			privateKeyExpect, err := parsePrivateKey(tc.e.SaPrivateKeyRaw)
			if string(tc.e.SaPrivateKeyRaw) != "" {
				assert.NoError(t, err)
			}

			ickExpect := iamStaticKey{
				SaId:         tc.e.SaId,
				SaKeyId:      tc.e.SaKeyId,
				SaPrivateKey: privateKeyExpect,
			}

			egm := newEnvGetterMock()
			egm.AddStrings(tc.i.Envs...)

			frm := newFileReaderMock()
			frm.AddFile(tc.i.FileName, tc.i.FileData)

			ick := iamStaticKey{}
			err = ick.getFromEnvGetter(egm, frm)
			if tc.e.HaveError {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, ickExpect, ick)
		})
	}
}
