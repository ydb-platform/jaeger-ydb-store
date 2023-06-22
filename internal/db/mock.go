package db

type envGetterMock struct {
	storage map[string]string
}

func newEnvGetterMock() *envGetterMock {
	egm := envGetterMock{}
	egm.storage = make(map[string]string)
	return &egm
}

func (egm *envGetterMock) GetString(key string) string {
	result, _ := egm.storage[key]
	return result
}

func (egm *envGetterMock) AddStrings(args ...[2]string) {
	for _, arg := range args {
		egm.storage[arg[0]] = arg[1]
	}
}
