package openapi

import (
	"net/http"
	"time"
)

type Context struct {
	Request *http.Request
}

// Done implements context.Context.
func (c Context) Done() <-chan struct{} {
	return c.Request.Context().Done()
}

// Err implements context.Context.
func (c Context) Err() error {
	return c.Request.Context().Err()
}

// Value implements context.Context.
func (c Context) Value(key any) any {
	return c.Request.Context().Value(key)
}

func (c Context) Deadline() (deadline time.Time, ok bool) {
	return c.Request.Context().Deadline()
}
