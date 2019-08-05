package blockfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

type writer struct {
	fs    *FS
	bsize int

	buf bytes.Buffer
}

func (w *writer) flush(data []byte) error {
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])

	if w.fs.Exists(id) {
		return nil
	}

	err := os.MkdirAll(filepath.Join(w.fs.root, id[:2]), 0700)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(
		filepath.Join(w.fs.root, id[:2], id),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func (w *writer) shift(buf []byte) {
	w.buf.Read(buf)

	rem := w.buf.Bytes()
	w.buf.Reset()
	w.buf.Write(rem)
}

func (w *writer) Write(data []byte) (n int, err error) {
	w.buf.Write(data)
	for w.buf.Len() >= w.bsize {
		buf := make([]byte, w.bsize)
		w.shift(buf)
		err := w.flush(buf)
		if err != nil {
			return len(data), err
		}
	}

	return len(data), nil
}

func (w *writer) Close() error {
	return w.flush(w.buf.Bytes())
}
