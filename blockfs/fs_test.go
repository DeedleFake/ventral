package blockfs_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/DeedleFake/ventral/blockfs"
)

const testFile = `This is a test.
This is also a test, oddly enough.
This is a test, too.
`

func TestFS(t *testing.T) {
	fs, err := blockfs.Open("testdata")
	if err != nil {
		t.Fatal(err)
	}

	var blocks []string
	t.Run("Write", func(t *testing.T) {
		w := fs.Write(blockfs.NewRabinChunker((1<<5)-1, 101))
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
		r, err := fs.Read(blocks)
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
	c := blockfs.NewRabinChunker((1<<5)-1, 101)
	io.WriteString(c, testFile)

	lines := [][]byte{
		[]byte("This is a test.\nThis is also a te"),
		[]byte("st, oddly enough.\nThi"),
	}

	for _, line := range lines {
		chunk := c.Next(false)
		if len(chunk) == 0 {
			break
		}

		if !bytes.Equal(chunk, line) {
			t.Errorf("Expected %q\nGot %q", line, chunk)
		}
	}

	lines = [][]byte{
		[]byte("s is a test, too.\n"),
		[]byte(""),
	}

	for _, line := range lines {
		chunk := c.Next(true)
		if len(chunk) == 0 {
			break
		}

		if !bytes.Equal(chunk, line) {
			t.Errorf("Expected %q\nGot %q", line, chunk)
		}
	}
}
