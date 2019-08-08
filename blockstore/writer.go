package blockstore

import (
	"crypto/sha1"
	"encoding/hex"
	"path/filepath"
)

// Writer writes to a store, automatically deduplicating blocks.
type Writer struct {
	fs      FileSystem
	chunker Chunker
	blocks  []string
	err     error
}

// Write returns a Writer that writes a file into the store,
// automatically deduplicating data into the appropriate blocks using
// the specified Chunker. It must be closed when all data has been
// written to it to make sure that partial blocks can be written
// properly.
func Write(fs FileSystem, chunker Chunker) *Writer {
	return &Writer{
		fs:      fs,
		chunker: chunker,
	}
}

func (w *Writer) flush(data []byte) (err error) {
	sum := sha1.Sum(data)
	id := hex.EncodeToString(sum[:])

	defer func() {
		if err == nil {
			w.blocks = append(w.blocks, id)
		}
	}()

	if w.fs.Exists(id) {
		return nil
	}

	file, err := w.fs.Create(filepath.Join(id[:2], id))
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

func (w *Writer) Write(data []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}

	n, err = w.chunker.Write(data)
	if err != nil {
		return n, err
	}

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

func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}

	return w.flush(w.chunker.Next(true))
}

// Blocks returns a list of the hashes of blocks that have been
// written by this Writer. Its return is only particularly useful
// after the Writer has been written to and closed with no errors.
func (w *Writer) Blocks() []string {
	return w.blocks
}
