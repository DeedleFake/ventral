package blockfs_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/DeedleFake/ventral/blockfs"
)

func TestReader(t *testing.T) {
	const testFile = `This is a test.
This is also a test, oddly enough.
This is a test, too.
`

	fs, err := blockfs.Open("testdata")
	if err != nil {
		t.Error(err)
	}

	t.Run("Write", func(t *testing.T) {
		w := fs.Write(16)
		defer func() {
			err := w.Close()
			if err != nil {
				t.Error(err)
			}
		}()

		_, err := io.WriteString(w, testFile)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Read", func(t *testing.T) {
		r, err := fs.Read([]string{
			"11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1",
			"2e51da166e5cb61d0c09befc72c257e086ee6a0dc9f34db3a87063c008251b95",
			"be7157b38ebac8762493f5a81405396accb778e90b51b8305f26248ba07607c7",
		})
		if err != nil {
			t.Error(err)
		}
		defer func() {
			err := r.Close()
			if err != nil {
				t.Error(err)
			}
		}()

		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error(err)
		}

		if string(data) != testFile {
			t.Errorf("Data did not match test file: %q", data)
		}
	})
}
