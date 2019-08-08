package blockstore

import (
	"io"
	"os"
	"path/filepath"
)

type Reader struct {
	// Wrap inserts an io.Reader into the pipeline in between the OS's
	// filesystem and the client. This is useful for, for example,
	// decompressing blocks that were compressed.
	Wrap func(io.Reader) (io.Reader, error)

	root    string
	blocks  []string
	curFile *os.File
	cur     io.Reader
}

// TODO: Verify the blocks that are being read.
func (r *Reader) Read(buf []byte) (n int, err error) {
	if r.blocks == nil {
		return 0, os.ErrClosed
	}

	if len(r.blocks) == 0 {
		return 0, io.EOF
	}

	if r.curFile == nil {
		cur, err := os.Open(filepath.Join(r.root, r.blocks[0][:2], r.blocks[0]))
		if err != nil {
			return 0, err
		}

		r.curFile = cur
		r.cur = r.curFile
		if r.Wrap != nil {
			r.cur, err = r.Wrap(r.cur)
			if err != nil {
				r.curFile = nil
				r.cur = nil
				return 0, err
			}
		}
		r.blocks = r.blocks[1:]
	}

	n, err = r.cur.Read(buf)
	if err == io.EOF {
		err := r.curFile.Close()
		if err != nil {
			return n, err
		}
		r.curFile = nil
		r.cur = nil

		if n == len(buf) {
			return n, err
		}

		sub, err := r.Read(buf[n:])
		n += sub
		return n, err
	}

	return n, err
}

func (r *Reader) Close() error {
	if r.curFile != nil {
		err := r.curFile.Close()
		if err != nil {
			return err
		}
		r.curFile = nil
		r.cur = nil
	}

	r.blocks = nil
	return nil
}
