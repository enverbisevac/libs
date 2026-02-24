package pgblob

import (
	"io"

	"gocloud.dev/blob/driver"
)

type reader struct {
	r     io.Reader
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *reader) Close() error {
	return nil
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i any) bool { return false }
