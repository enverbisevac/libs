package errors

import (
	"cmp"
	"encoding/json"
	"errors"
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
		_ = fn(b.StatusCode)
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

func (r *HttpResponse) AsError() error {
	errSlice := make(MarshalableErrors, len(r.Errors))
	for i, v := range r.Errors {
		errSlice[i] = errors.New(v)
	}
	switch r.Status {
	case http.StatusBadRequest:
		return &ValidationError{Base: r.Base, Errors: errSlice}
	case http.StatusUnauthorized:
		return &UnauthenticatedError{Base: r.Base}
	case http.StatusForbidden:
		return &UnauthorizedError{Base: r.Base}
	case http.StatusNotFound:
		return &NotFoundError{Base: r.Base}
	case http.StatusConflict:
		return &ConflictError{Base: r.Base, Errors: errSlice}
	case http.StatusUnprocessableEntity:
		return &ValidationError{Base: r.Base, Errors: errSlice}
	case http.StatusInternalServerError:
		return &InternalError{Base: r.Base}
	case http.StatusNotImplemented:
		return &NotImplementedError{Base: r.Base}
	default:
		return &InternalError{Base: r.Base}
	}
}

func JSONResponse(w http.ResponseWriter, err error, options ...JSONResponseOption) error {
	w.Header().Set("Content-Type", "application/problem+json")
	if err == nil {
		return nil
	}
	orgErr := err
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

	w.WriteHeader(http.StatusInternalServerError)
	return json.NewEncoder(w).Encode(HttpResponse{
		Base:   newBase(orgErr.Error()),
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
	origErr := err
again:
	v, ok := err.(httpResponse)
	if ok {
		response := v.HttpResponse()
		w.WriteHeader(response.Status)
		_ = encoder.Encode(response)
		return
	}
	err = Unwrap(err)
	if err != nil {
		goto again
	}

	_ = encoder.Encode(HttpResponse{
		Base:   newBase(cmp.Or(err, origErr).Error()),
		Status: http.StatusInternalServerError,
	})
}
