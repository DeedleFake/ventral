package ventral

import (
	"io"
	"net/http"
	"net/url"
	"path"
)

func Read(c *http.Client, u *url.URL, id string) (r io.ReadCloser, err error) {
	uc := *u

	uc.Path = path.Join(uc.Path, "read")

	q := uc.Query()
	q.Set("id", id)
	uc.RawQuery = q.Encode()

	rsp, err := c.Get(uc.String())
	if err != nil {
		return nil, err
	}

	return rsp.Body, nil
}

func Write(c *http.Client, u *url.URL, r io.Reader) error {
	uc := *u

	uc.Path = path.Join(uc.Path, "write")

	_, err := c.Post(uc.String(), "application/octet-stream", r)
	return err
}
