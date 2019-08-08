package blockstore_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/DeedleFake/ventral/blockstore"
)

const testFile = `This is a test.
This is also a test, oddly enough.
This is a test, too.
`

func TestStore(t *testing.T) {
	fs := blockstore.Gzip(blockstore.Dir("testdata"))

	var blocks []string
	t.Run("Write", func(t *testing.T) {
		w := blockstore.Write(fs, blockstore.NewRabinChunker((1<<4)-1, 101))
		defer func() {
			err := w.Close()
			if err != nil {
				t.Fatal(err)
			}

			blocks = w.Blocks()
		}()

		_, err := io.WriteString(w, testFile)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Read", func(t *testing.T) {
		r, err := blockstore.Read(fs, blocks)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != testFile {
			t.Fatalf("Data did not match test file: %q", data)
		}
	})
}

func TestChunker(t *testing.T) {
	c := blockstore.NewRabinChunker((1<<4)-1, 101)
	io.WriteString(c, testFile)

	lines := [][]byte{
		[]byte("This is a test.\nTh"),
		[]byte("is is also a test, oddly enough.\nThis is a test, too."),
	}

	for i := 0; i < 10; i++ {
		chunk := c.Next(false)
		if len(chunk) == 0 {
			if i < len(lines) {
				t.Errorf("Not enough chunks: %v", i)
			}
			break
		}

		if i >= len(lines) {
			t.Errorf("Extra chunk: %q", chunk)
			continue
		}

		if !bytes.Equal(chunk, lines[i]) {
			t.Errorf("Expected %q\nGot %q", lines[i], chunk)
		}
	}

	lines = [][]byte{
		[]byte("\n"),
	}

	for i := 0; i < 10; i++ {
		chunk := c.Next(true)
		if len(chunk) == 0 {
			if i < len(lines) {
				t.Errorf("Not enough chunks: %v", i)
			}
			break
		}

		if i >= len(lines) {
			t.Errorf("Extra chunk: %q", chunk)
			continue
		}

		if !bytes.Equal(chunk, lines[i]) {
			t.Errorf("Expected %q\nGot %q", lines[i], chunk)
		}
	}
}
