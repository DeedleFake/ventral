package blockfs

type writer struct {
	root string
}

func (w *writer) Write(buf []byte) (n int, err error) {
	panic("Not implemented.")
}

func (w *writer) Close() error {
	panic("Not implemented.")
}
