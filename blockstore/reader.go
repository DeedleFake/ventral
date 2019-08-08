package blockstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var (
	// ErrNoSuchBlock is returned when a block is expected to exist and
	// doesn't.
	ErrNoSuchBlock = errors.New("block does not exist")
)

type Reader struct {
	fs     FileSystem
	blocks []string
	cur    io.ReadCloser
}

// Read returns a Reader that reads from the given blocks in order. It
// only keeps a single block file open at a time. Closing the returned
// io.ReadCloser closes the currently open file and causes further
// reads to return errors.
func Read(fs FileSystem, blocks []string) (*Reader, error) {
	for _, block := range blocks {
		if (len(block) < 2) || !fs.Exists(filepath.Join(block[:2], block)) {
			return nil, ErrNoSuchBlock
		}
	}

	if blocks == nil {
		blocks = []string{}
	}

	return &Reader{
		fs:     fs,
		blocks: blocks,
	}, nil
}

// TODO: Verify the blocks that are being read.
func (r *Reader) Read(buf []byte) (n int, err error) {
	if r.blocks == nil {
		return 0, os.ErrClosed
	}

	if len(r.blocks) == 0 {
		return 0, io.EOF
	}

	if r.cur == nil {
		cur, err := r.fs.Open(filepath.Join(r.blocks[0][:2], r.blocks[0]))
		if err != nil {
			return 0, err
		}

		r.cur = cur
		r.blocks = r.blocks[1:]
	}

	n, err = r.cur.Read(buf)
	if err == io.EOF {
		err := r.cur.Close()
		if err != nil {
			return n, err
		}
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
