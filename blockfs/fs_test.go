package blockfs_test

import (
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

	r, err := fs.Read([]string{
		"11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1",
		"2e51da166e5cb61d0c09befc72c257e086ee6a0dc9f34db3a87063c008251b95",
		"be7157b38ebac8762493f5a81405396accb778e90b51b8305f26248ba07607c7",
	})
	if err != nil {
		t.Error(err)
	}
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
	}

	if string(data) != testFile {
		t.Errorf("Data did not match test file: %q", data)
	}
}
