package main

import (
	"io"
	"os"
)

type lazyFile struct {
	path string
	file *os.File
}

func (f *lazyFile) Read(buf []byte) (n int, err error) {
	if f.file == nil {
		file, err := os.Open(f.path)
		if err != nil {
			return 0, err
		}

		f.file = file
	}

	n, err = f.file.Read(buf)
	if err == io.EOF {
		cerr := f.file.Close()
		if cerr != nil {
			return n, cerr
		}
	}

	return n, err
}

func (f *lazyFile) Close() error {
	if f.file == nil {
		return nil
	}

	return f.file.Close()
}
