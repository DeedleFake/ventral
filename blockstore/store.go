// Package blockstore provides utilities for interfacing block-based storage.
//
// ventral's block storage system stores data as collections of
// blocks. Each block is stored in a filename of its own hash, inside
// of a hash-prefix subdirectory, and a "file" is an ordered
// collection of block hashes. In other words, a "file" might look
// like
//
//    11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1
//    d56689f1e89a5029edf549153a4df0419343e0025a92a91a086d2225e26a8938
//
// and this file would be stored on disk as
//
//    <store root>
//     |-- 11/
//     |    |-- 11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1
//     |-- d5/
//          |-- d56689f1e89a5029edf549153a4df0419343e0025a92a91a086d2225e26a8938
//
// Reading this file would consist of essentially concatenating the
// contents of these two blocks.
package blockstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var prefixRE = regexp.MustCompile(`^[0-9a-f]{2}$`)

var (
	// ErrNoSuchBlock is returned when a block is expected to exist and
	// doesn't.
	ErrNoSuchBlock = errors.New("block does not exist in filesystem")
)

// Store provides an interface for dealing with a block storage
// system. It is not safe for concurrent use.
type Store struct {
	root     string
	prefixes map[string]struct{}
}

// Open opens the filesystem rooted at the specified path.
func Open(root string) (*Store, error) {
	err := os.MkdirAll(root, 0700)
	if err != nil {
		return nil, err
	}

	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	s := &Store{
		root:     root,
		prefixes: make(map[string]struct{}),
	}

	for {
		infos, err := dir.Readdir(32)
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		for _, info := range infos {
			if len(info.Name()) != 2 {
				continue
			}

			if !prefixRE.MatchString(info.Name()) {
				continue
			}

			if !info.IsDir() {
				return nil, fmt.Errorf("prefix directory %q is a file", info.Name())
			}

			s.addPrefix(info.Name())
		}
	}

	return s, nil
}

func (s *Store) addPrefix(id string) {
	s.prefixes[id[:2]] = struct{}{}
}

func (s *Store) hasPrefix(id string) bool {
	_, ok := s.prefixes[id[:2]]
	return ok
}

// Exists returns true if a block exists. It does not check if the
// block is valid or not.
func (s *Store) Exists(block string) bool {
	if !s.hasPrefix(block) {
		return false
	}

	_, err := os.Lstat(filepath.Join(s.root, block[:2], block))
	return !os.IsNotExist(err)
}

// Read returns an io.ReadCloser that reads from the given blocks in
// order. It only keeps a single block file open at a time. Closing
// the returned io.ReadCloser closes the currently open file and
// causes further reads to return errors.
func (s *Store) Read(blocks []string) (*Reader, error) {
	for _, block := range blocks {
		if !s.Exists(block) {
			return nil, ErrNoSuchBlock
		}
	}

	if blocks == nil {
		blocks = []string{}
	}

	return &Reader{
		root:   s.root,
		blocks: blocks,
	}, nil
}

// Write returns an Writer that writes a file into the store,
// automatically deduplicating data into the appropriate blocks using
// the specified Chunker. It must be closed when all data has been
// written to it to make sure that partial blocks can be written
// properly.
func (s *Store) Write(chunker Chunker) *Writer {
	return &Writer{
		s:       s,
		chunker: chunker,
	}
}
