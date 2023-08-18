package db

type credentialsType int

const (
	anonymousCredentials = iota
	accessTokenCredentials
	metadataCredentials
	saKeyDeprecatedCredentials
	saKeyJsonCredentials
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
	case saKeyJsonCredentials:
		return "SaKeyJsonCredentials"
	default:
		return "unspecified"
	}
}
