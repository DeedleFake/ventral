// Package blockstore provides mechanisms for interacting with a
// content-IDed block-storage system.
package blockstore

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// Store provides an abstracted interface to a block storage system.
// Blocks are IDed as strings containing cryptographic hashes of the
// block's contents.
type Store interface {
	// Write stores a block into the blockstore. Writing to the returned
	// writer writes the data into a temporary location, and closing it
	// commits it to the store. The returned byte slice will be
	// populated with the block ID after each write as raw bytes. For
	// actual use, the client will likely want to encode this ID to a
	// hex string.
	Write() (w io.WriteCloser, id []byte, err error)

	// Read returns a reader which reads a block from the store.
	Read(str string) (r io.ReadCloser, err error)
}

type dir string

// Dir returns a Store which stores data on the filesystem under the
// directory specified. Block IDs are generated from SHA-1 hashes of
// the their contents and are stored in subdirectories of root named
// for the block's prefix. Be aware that this function will generate
// these subdirectories immediately, as well as a directory called
// "write" into which data is written temporarily while the hash is
// being generated. Once a writer returned by the Write method has
// been closed, the file in the "write" directory is moved over into
// the appropriate directory and renamed to the hash ID.
func Dir(root string) (Store, error) {
	for i := int64(0); i < 256; i++ {
		var pad string
		if i < 16 {
			pad = "0"
		}

		err := os.MkdirAll(filepath.Join(root, pad+strconv.FormatInt(i, 16)), 0700)
		if err != nil {
			return nil, err
		}
	}

	err := os.MkdirAll(filepath.Join(root, "write"), 0700)
	if err != nil {
		return nil, err
	}

	return dir(root), nil
}

func (d dir) path(parts ...string) string {
	return filepath.Join(string(d), filepath.Join(parts...))
}

func (d dir) Write() (io.WriteCloser, []byte, error) {
	id := make([]byte, sha1.Size)
	_, err := io.ReadFull(rand.Reader, id)
	if err != nil {
		return nil, nil, err
	}

	name := hex.EncodeToString(id)
	file, err := os.Create(d.path("write", name))
	if err != nil {
		return nil, nil, err
	}

	h := sha1.New()
	h.Sum(id[:0])

	return &dirWriter{
		d:    d,
		name: name,
		w:    io.MultiWriter(file, h),
		c:    file,
		h:    h,
		id:   id,
	}, id, nil
}

func (d dir) Read(id string) (io.ReadCloser, error) {
	if len(id) < 2 {
		return nil, errors.New("ID is too short")
	}

	return os.Open(d.path(id[:2], id))
}

type dirWriter struct {
	d    dir
	name string
	w    io.Writer
	c    io.Closer
	err  error
	h    hash.Hash
	id   []byte
}

func (w *dirWriter) Write(data []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}

	n, err = w.w.Write(data)
	if err != nil {
		w.err = err
		return n, err
	}

	w.h.Sum(w.id[:0])
	return n, nil
}

func (w *dirWriter) Close() error {
	err := w.c.Close()
	if err != nil {
		return err
	}

	if w.err != nil {
		err := os.Remove(w.d.path("write", w.name))
		if err != nil {
			return err
		}

		return w.err
	}

	id := hex.EncodeToString(w.id)
	return os.Rename(w.d.path("write", w.name), w.d.path(id[:2], id))
}
