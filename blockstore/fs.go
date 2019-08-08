package blockstore

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
)

// A FileSystem provides an interface to the raw filesystem that the
// block store is actually stored on.
type FileSystem interface {
	// Open opens the file at path for reading.
	Open(path string) (io.ReadCloser, error)

	// Create creates a new file at path for writing to, as well as any
	// parent directories of that file. It fails if the file already
	// exists.
	Create(path string) (io.WriteCloser, error)

	// Exists returns true if the file at the given path can be
	// determined to exist or not.
	Exists(path string) bool
}

type dir string

// Dir returns a FileSystem that uses the operating system's
// filesystem and is rooted in the given directory.
func Dir(root string) FileSystem {
	return dir(root)
}

func (d dir) Open(path string) (io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(string(d), path))
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (d dir) Create(path string) (io.WriteCloser, error) {
	err := os.MkdirAll(filepath.Join(string(d), filepath.Dir(path)), 0700)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Join(string(d), path), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (d dir) Exists(path string) bool {
	_, err := os.Lstat(filepath.Join(string(d), path))
	return err == nil
}

type gzipFS struct {
	FileSystem
}

// Gzip returns a FileSystem that gzips and gunzips all the data
// written to and read from the given FS.
func Gzip(fs FileSystem) FileSystem {
	return &gzipFS{FileSystem: fs}
}

func (g gzipFS) Open(path string) (io.ReadCloser, error) {
	r, err := g.FileSystem.Open(path)
	if err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(r)
	if err != nil {
		r.Close()
		return nil, err
	}

	return &wrappedFile{r: gr, c: r}, nil
}

func (g gzipFS) Create(path string) (io.WriteCloser, error) {
	w, err := g.FileSystem.Create(path)
	if err != nil {
		return nil, err
	}

	gw := gzip.NewWriter(w)
	return &wrappedFile{w: gw, c: w}, nil
}

type wrappedFile struct {
	r io.ReadCloser
	w io.WriteCloser
	c io.Closer
}

func (file wrappedFile) Read(buf []byte) (int, error) {
	return file.r.Read(buf)
}

func (file wrappedFile) Write(data []byte) (int, error) {
	return file.w.Write(data)
}

func (file wrappedFile) Close() error {
	if file.r != nil {
		err := file.r.Close()
		if err != nil {
			return err
		}
	}

	if file.w != nil {
		err := file.w.Close()
		if err != nil {
			return err
		}
	}

	return file.c.Close()
}
