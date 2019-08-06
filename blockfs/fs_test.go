package blockfs_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/DeedleFake/ventral/blockfs"
)

func TestFS(t *testing.T) {
	const testFile = `This is a test.
This is also a test, oddly enough.
This is a test, too.
`

	fs, err := blockfs.Open("testdata")
	if err != nil {
		t.Fatal(err)
	}

	var blocks []string
	t.Run("Write", func(t *testing.T) {
		w := fs.Write(16)
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
