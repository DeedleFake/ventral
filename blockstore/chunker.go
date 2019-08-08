package blockstore

import (
	"io"
)

// A Chunker splits a stream of data into chunks.
type Chunker interface {
	// Write adds more data to the internal buffer for chunking. It
	// never returns an error.
	io.Writer

	// Next returns the next chunk. If end is false, it will return a
	// zero-length slice at the end of the internal buffer and this
	// operation will not advance to the beginning of the next chunk. If
	// it is true then it will consider the end of the buffer to be a
	// chunk boundary.
	//
	// The returned byte slice is only valid until the next call to
	// Next.
	Next(end bool) []byte
}

type rabinChunker struct {
	mask  uint64
	prime uint64
	mult  uint64

	buf []byte
	loc int
}

// NewRabinChunker returns a Chunker that finds chunk boundaries using
// a Rabin fingerprint mask. For example, to produce chunks that are
// approximately 8KB, use a mask of (1 << 13) - 1.
func NewRabinChunker(mask uint64, prime uint64) Chunker {
	mult := uint64(1)
	for i := 0; i < 64; i++ {
		mult = (mult * 256) % prime
	}

	return &rabinChunker{
		mask:  uint64(mask),
		prime: prime,
		mult:  mult,
	}
}

func (c *rabinChunker) Write(data []byte) (n int, err error) {
	c.buf = append(c.buf, data...)
	return len(data), nil
}

func (c *rabinChunker) WriteString(data string) (n int, err error) {
	c.buf = append(c.buf, data...)
	return len(data), nil
}

func (c *rabinChunker) Next(end bool) []byte {
	if c.loc > 0 {
		n := copy(c.buf, c.buf[c.loc:])
		c.buf = c.buf[:n]
	}

	var sum uint64
	mult := uint64(1)
	for c.loc = 0; (c.loc < len(c.buf)) && (c.loc < 64); c.loc++ {
		sum = (sum*256)%c.prime + uint64(c.buf[c.loc])
		mult = (sum * 256) % c.prime
	}
	for ; c.loc < len(c.buf); c.loc++ {
		if sum&c.mask == 0 {
			return c.buf[:c.loc]
		}

		sum -= mult * uint64(c.buf[c.loc-63])
		sum = (sum*256)%c.prime + uint64(c.buf[c.loc])
	}

	if end {
		return c.buf
	}

	c.loc = 0
	return nil
}
