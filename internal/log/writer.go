package log

import (
	"fmt"
	"os"
)

type stderrFileWriter struct {
	filePath string
}

func newStderrFileWriter(filePath string) (*stderrFileWriter, error) {
	if filePath == "" {
		return &stderrFileWriter{}, nil
	}
	result := &stderrFileWriter{filePath: filePath}
	return result, nil

}

func (cw *stderrFileWriter) Write(p []byte) (n int, err error) {
	_, err = os.Stderr.Write(p)
	if err != nil {
		return 0, err
	}
	if cw.filePath != "" {
		file, err := os.OpenFile(cw.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("BAD", err)
			return 0, err
		}
		defer func() {
			err = file.Close()
		}()

		_, err = file.Write(p)
		if err != nil {
			fmt.Println("BAD", err)
			return 0, err
		}
	}
	return 0, nil
}
