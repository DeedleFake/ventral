package ventral

import (
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// Read sends a read request to a ventral server for the block with
// the given ID. It returns a reader that reads the block's contents.
// It is the caller's responsibility to close the reader.
//
// The URL to which the request is sent is built from the given URL by
// adding only that which is necessary to make the request. It does so
// without examining the existing URL, so the URL should point to the
// root path of the API being contacted. In other words, if the API is
// hosted under http://www.example.com/ventral/api, then that is the
// URL which should be provided to this function.
func Read(c *http.Client, apiURL string, id string) (r io.ReadCloser, err error) {
	u, err := url.Parse(apiURL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "read")

	q := u.Query()
	q.Set("id", id)
	u.RawQuery = q.Encode()

	rsp, err := c.Get(u.String())
	if err != nil {
		return nil, err
	}

	return rsp.Body, nil
}

// Write sends a write request to a ventral server to create a new
// block containing the data yielded by r. It returns the block's ID.
//
// The URL to which the request is sent is built from the given URL by
// adding only that which is necessary to make the request. It does so
// without examining the existing URL, so the URL should point to the
// root path of the API being contacted. In other words, if the API is
// hosted under http://www.example.com/ventral/api, then that is the
// URL which should be provided to this function.
func Write(c *http.Client, apiURL string, r io.Reader) (id string, err error) {
	u, err := url.Parse(apiURL)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "write")

	rsp, err := c.Post(u.String(), "application/octet-stream", r)
	if err != nil {
		return "", err
	}
	defer rsp.Body.Close()

	var sb strings.Builder
	_, err = io.Copy(&sb, rsp.Body)
	return sb.String(), err
}
