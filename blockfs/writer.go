package blockfs

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

// Writer writes to an FS, automatically deduplicating blocks.
type Writer interface {
	io.WriteCloser

	// Blocks returns a list of the hashes of blocks that have been
	// written by this Writer. Its return is only particularly useful
	// after the Writer has been written to and closed with no errors.
	Blocks() []string
}

type writer struct {
	fs *FS

	chunker Chunker
	blocks  []string
	err     error
}

func (w *writer) flush(data []byte) (err error) {
	sum := sha1.Sum(data)
	id := hex.EncodeToString(sum[:])

	defer func() {
		if err == nil {
			w.blocks = append(w.blocks, id)
			w.fs.addPrefix(id)
		}
	}()

	if w.fs.Exists(id) {
		return nil
	}

	err = os.MkdirAll(filepath.Join(w.fs.root, id[:2]), 0700)
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
	defer func() {
		e := file.Close()
		if (e != nil) && (err == nil) {
			err = e
		}
	}()

	_, err = file.Write(data)
	return err
}

func (w *writer) Write(data []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}

	w.chunker.Write(data)

	for {
		chunk := w.chunker.Next(false)
		if len(chunk) == 0 {
			break
		}

		err := w.flush(chunk)
		if err != nil {
			w.err = err
			return len(data), err
		}
	}

	return len(data), nil
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	return w.flush(w.chunker.Next(true))
}

func (w *writer) Blocks() []string {
	return w.blocks
}
