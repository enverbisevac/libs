package errors

import (
	"encoding/json"
	"net/http"
)

type httpResponse interface {
	HttpResponse() HttpResponse
}

type JSONResponseFunc func(*Base)

type JSONResponseOption interface {
	Apply(*Base)
}

func (f JSONResponseFunc) Apply(b *Base) {
	f(b)
}

func ProcessStatus(fn func(status string) error) JSONResponseFunc {
	return func(b *Base) {
		fn(b.StatusCode)
	}
}

func WithTraceID(traceID string) JSONResponseFunc {
	return func(b *Base) {
		b.TraceID = traceID
	}
}

type HttpResponse struct {
	Base
	Status int      `json:"-"`
	Errors []string `json:"errors"`
}

func JSONResponse(w http.ResponseWriter, err error, options ...JSONResponseOption) error {
	w.Header().Set("Content-Type", "application/problem+json")
	if err == nil {
		return nil
	}
again:
	v, ok := err.(httpResponse)
	if ok {
		response := v.HttpResponse()
		w.WriteHeader(response.Status)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			return err
		}
		return nil
	}
	err = Unwrap(err)
	if err != nil {
		goto again
	}

	return json.NewEncoder(w).Encode(HttpResponse{
		Base:   NewBase(err.Error()),
		Status: http.StatusInternalServerError,
	})
}

type Encoder interface {
	Encode(v any) error
}

func Response(encoder Encoder, w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
again:
	v, ok := err.(httpResponse)
	if ok {
		response := v.HttpResponse()
		w.WriteHeader(response.Status)
		encoder.Encode(response)
		return
	}
	err = Unwrap(err)
	if err != nil {
		goto again
	}

	encoder.Encode(HttpResponse{
		Base:   NewBase(err.Error()),
		Status: http.StatusInternalServerError,
	})
}
