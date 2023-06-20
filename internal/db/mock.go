package db

import "errors"

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

type fileReaderMock struct {
	storage map[string][]byte
}

func newFileReaderMock() *fileReaderMock {
	frm := fileReaderMock{}
	frm.storage = make(map[string][]byte)
	return &frm
}

func (frm *fileReaderMock) ReadFile(name string) ([]byte, error) {
	result, ok := frm.storage[name]
	if !ok {
		return nil, errors.New("file not found")
	}
	return result, nil
}

func (frm *fileReaderMock) AddFile(name string, data []byte) {
	frm.storage[name] = data
}
