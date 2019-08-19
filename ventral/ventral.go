// Package ventral provides utilities for utilizing the ventral API.
package ventral

import (
	"fmt"
	"io"
	"net/http"

	"github.com/DeedleFake/ventral/blockstore"
)

func write(store blockstore.Store, r io.Reader) (id string, err error) {
	w, wid, err := store.Write()
	if err != nil {
		return "", err
	}
	defer func() {
		e := w.Close()
		if (e != nil) && (err == nil) {
			err = e
		}

		id = *wid
	}()

	_, err = io.Copy(w, r)
	return "", err
}

func read(store blockstore.Store, id string, w io.Writer) (err error) {
	r, err := store.Read(id)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(w, r)
	return err
}

// Handler returns an HTTP handler which serves a ventral server using
// the given blockstore.
func Handler(store blockstore.Store) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "write":
			id, err := write(store, req.Body)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(rw, "Error: %v", err)
				break
			}
			fmt.Fprintf(rw, "%s", id)

		case "read":
			err := read(store, req.URL.Query().Get("id"), rw)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(rw, "Error: %v", err)
				break
			}

		default:
			rw.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(rw, "Error: invalid request: %v", req.URL.Path)
		}
	})
}
