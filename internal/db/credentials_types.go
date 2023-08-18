package db

type credentialsType int

const (
	anonymousCredentials = iota
	accessTokenCredentials
	metadataCredentials
	saKeyDeprecatedCredentials
	SaKeyJsonCredentials
)

func (c credentialsType) String() string {
	switch c {
	case anonymousCredentials:
		return "anonymousCredentials"
	case accessTokenCredentials:
		return "accessTokenCredentials"
	case metadataCredentials:
		return "metadataCredentials"
	case saKeyDeprecatedCredentials:
		return "saKeyDeprecatedCredentials"
	case SaKeyJsonCredentials:
		return "SaKeyJsonCredentials"
	default:
		return "unspecified"
	}
}
