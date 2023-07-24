package log

import (
	"fmt"
	"os"
)

type StderrFileWriter struct {
	file *os.File
}

func newCustomWriter(filePath string) (*StderrFileWriter, error) {
	if filePath == "" {
		return &StderrFileWriter{}, nil
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModeAppend)
	if err != nil {
		return &StderrFileWriter{}, err
	}

	result := &StderrFileWriter{file: file}
	return result, nil
}

func (cw *StderrFileWriter) Write(p []byte) (n int, err error) {
	_, err = os.Stderr.Write(p)
	if err != nil {
		return 0, err
	}
	if cw.file != nil {
		_, err = cw.file.Write(p)
		if err != nil {

			fmt.Println("BAD", err)
			return 0, err
		}
	}
	return 0, nil
}

func (cw *StderrFileWriter) Close() error {
	return cw.file.Close()
}
