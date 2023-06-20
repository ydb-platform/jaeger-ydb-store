package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_iamStaticKey_getFromEnvGetter(t *testing.T) {

	t.Run("smoke", func(t *testing.T) {
		privateKeyRaw := []byte("-----BEGIN RSA PRIVATE KEY-----\nMCsCAQACBHbKUj0CAwEAAQIEVeGJhQIDANj7AgMAjCcCAwCOdwICTGsCAm+5\n-----END RSA PRIVATE KEY-----\n")
		privateKey, _ := parsePrivateKey(privateKeyRaw)
		expect := iamStaticKey{
			SaId:         "biba_id",
			SaKeyId:      "biba_key_id",
			SaPrivateKey: privateKey,
		}

		egm := newEnvGetterMock()
		egm.AddStrings(
			[2]string{KeyYdbSaId, "biba_id"},
			[2]string{KeyYdbSaKeyID, "biba_key_id"},
			[2]string{KeyYdbSaPrivateKeyFile, "ydb.key"})

		frm := newFileReaderMock()
		frm.AddFile("ydb.key", privateKeyRaw)

		ick := iamStaticKey{}
		err := ick.getFromEnvGetter(egm, frm)
		assert.NoError(t, err)
		assert.Equal(t, expect, ick)
	})

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
			"smoke",
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
			}

			assert.Equal(t, ickExpect, ick)
		})
	}

}
