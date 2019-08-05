package blockfs

import (
	"io"
	"os"
	"path/filepath"
)

type reader struct {
	root   string
	blocks []string
	cur    *os.File
}

// Read returns an io.ReadCloser that reads from the given blocks in
// order relative to the specified FS root. It only keeps a single
// block file open at a time. Closing the returned io.ReadCloser
// closes the currently open file and causes further reads to return
// errors.
func Read(root string, blocks []string) io.ReadCloser {
	if blocks == nil {
		blocks = []string{}
	}

	return &reader{
		root:   root,
		blocks: blocks,
	}
}

// TODO: Verify the blocks that are being read.
func (r *reader) Read(buf []byte) (n int, err error) {
	if r.blocks == nil {
		return 0, os.ErrClosed
	}

	if len(r.blocks) == 0 {
		return 0, io.EOF
	}

	if r.cur == nil {
		cur, err := os.Open(filepath.Join(r.root, r.blocks[0][:2], r.blocks[0]))
		if err != nil {
			return 0, err
		}

		r.cur = cur
		r.blocks = r.blocks[1:]
	}

	n, err = r.cur.Read(buf)
	if err == io.EOF {
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

func (r *reader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		if err != nil {
			return err
		}
		r.cur = nil
	}

	r.blocks = nil
	return nil
}
