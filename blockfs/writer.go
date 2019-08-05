package blockfs

import "io"

// Write returns an io.WriteCloser that writes a file into the FS
// located at root, automatically deduplicating data into the
// appropriate blocks.
func Write(root string) (io.WriteCloser, error) {
	panic("Not implemented.")
}
