package blockfs

import "bytes"

type writer struct {
	root  string
	bsize int

	buf bytes.Buffer
}

func (w *writer) flush(data []byte) error {
	panic("Not implemented.")
}

func (w *writer) shift(buf []byte) {
	_, _ = w.buf.Read(buf)

	rem := w.buf.Bytes()
	w.buf.Reset()
	w.buf.Write(rem)
}

func (w *writer) Write(data []byte) (n int, err error) {
	w.buf.Write(data)
	if w.buf.Len() < w.bsize {
		return len(data), nil
	}

	buf := make([]byte, w.bsize)
	w.shift(buf)
	return len(data), w.flush(buf)
}

func (w *writer) Close() error {
	return w.flush(w.buf.Bytes())
}
