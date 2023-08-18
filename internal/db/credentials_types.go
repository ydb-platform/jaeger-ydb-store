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
		return "AnonymousCredentials"
	case accessTokenCredentials:
		return "AccessTokenCredentials"
	case metadataCredentials:
		return "MetadataCredentials"
	case saKeyDeprecatedCredentials:
		return "SaKeyDeprecatedCredentials"
	case SaKeyJsonCredentials:
		return "SaKeyJsonCredentials"
	default:
		return "unspecified"
	}
}
