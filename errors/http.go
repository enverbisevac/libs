package errors

import (
	"encoding/json"
	"net/http"

	"github.com/enverbisevac/libs/ptr"
)

type status interface {
	HttpStatus() int
}

func HttpStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
again:
	v, ok := err.(status)
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
	var obj *Base
	w.Header().Set("Content-Type", "application/problem+json")
	status := HttpStatus(err)
	w.WriteHeader(status)
	if status == 0 {
		obj = ptr.From(NewBase(http.StatusInternalServerError, err.Error()))
	} else {
		obj = getBase(err)
	}

	for _, opt := range options {
		opt.Apply(obj)
	}

	if status == 0 {
		return json.NewEncoder(w).Encode(obj)
	}
	return json.NewEncoder(w).Encode(err)
}
