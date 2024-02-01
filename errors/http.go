package errors

import (
	"encoding/json"
	"net/http"

	"github.com/enverbisevac/libs/ptr"
)

type httpStatus interface {
	HttpStatus() int
}

func HttpStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
again:
	v, ok := err.(httpStatus)
	if ok {
		return v.HttpStatus()
	}
	err = Unwrap(err)
	if err == nil {
		return http.StatusInternalServerError
	}
	goto again
}

type baser interface {
	GetBase() *Base
}

func getBase(err error) *Base {
again:
	if err == nil {
		return nil
	}
	v, ok := err.(baser)
	if ok {
		return v.GetBase()
	}
	err = Unwrap(err)
	goto again
}

type JSONResponseFunc func(*Base)

type JSONResponseOption interface {
	Apply(*Base)
}

func (f JSONResponseFunc) Apply(b *Base) {
	f(b)
}

func ProcessStatus(fn func(status int) error) JSONResponseFunc {
	return func(b *Base) {
		fn(b.Status)
	}
}

func WithTraceID(traceID string) JSONResponseFunc {
	return func(b *Base) {
		b.TraceID = traceID
	}
}

func JSONResponse(w http.ResponseWriter, err error, options ...JSONResponseOption) error {
	w.Header().Set("Content-Type", "application/problem+json")
again:
	v, ok := err.(httpStatus)
	if ok {
		w.WriteHeader(v.HttpStatus())
		obj := getBase(err)
		for _, opt := range options {
			opt.Apply(obj)
		}
		return json.NewEncoder(w).Encode(v)
	}
	err = Unwrap(err)
	if err == nil {
		obj := ptr.From(NewBase(http.StatusInternalServerError, err.Error()))
		for _, opt := range options {
			opt.Apply(obj)
		}
		return json.NewEncoder(w).Encode(obj)
	}
	goto again
}
