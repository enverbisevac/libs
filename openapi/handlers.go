package openapi

import (
	"net/http"
	"reflect"

	"github.com/enverbisevac/libs/errors"
	"github.com/enverbisevac/libs/httputil"
	"github.com/go-chi/chi/v5"
)

// None is a marker type indicating "not applicable" for a type parameter.
type None struct{}

var noneType = reflect.TypeOf(None{})

// Req is a unified request type. H holds path/query/header params, B holds the request body.
// Use None for B when there is no request body.
type Req[H any, B any] struct {
	Header H
	Body   B
}

// Resp is a unified response type. S determines the HTTP status code, B holds the response body.
// Use None for B when there is no response body. Response headers can be set via Header.
type Resp[S Success, B any] struct {
	Header http.Header
	Status S
	Body   B
}

// HandlerFunc is the single handler signature for all operations.
type HandlerFunc[H, B any, S Success, R any] func(ctx Context, req Req[H, B]) (*Resp[S, R], error)

func (f HandlerFunc[H, B, S, R]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	encoder, decoder := GetEncDec(w, r)

	var req Req[H, B]

	// Decode path/query/header params
	decode := httputil.NewDecoder(httputil.PathValue(chi.URLParam)).Decode
	err := decode(r, &req.Header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	// Decode request body if B is not None
	if reflect.TypeOf((*B)(nil)).Elem() != noneType {
		err = decoder.Decode(&req.Body)
		if err != nil {
			errors.Response(encoder, w, err)
			return
		}
	}

	resp, err := f(Context{Request: r}, req)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	// Write response headers
	if resp.Header != nil {
		for k, v := range resp.Header {
			for _, val := range v {
				w.Header().Add(k, val)
			}
		}
	}

	// Write status code
	stat, ok := any(resp.Status).(HttpStatus)
	if ok {
		w.WriteHeader(stat.HttpStatus())
		if stat.HttpStatus() == http.StatusNoContent {
			return
		}
	}

	// Encode response body if R is not None
	if reflect.TypeOf((*R)(nil)).Elem() != noneType {
		err = encoder.Encode(resp.Body)
		if err != nil {
			errors.Response(encoder, w, err)
		}
	}
}

// Op creates an Operation from a unified HandlerFunc and a Meta config.
func Op[H, B any, S Success, R any](
	handler HandlerFunc[H, B, S, R],
	meta Meta,
) *Operation {
	op := &Operation{
		Handler:     handler,
		ID:          meta.ID,
		Summary:     meta.Summary,
		Description: meta.Description,
		Tags:        meta.Tags,
		Errors:      meta.Errors,
		Security:    meta.Security,
		handlers:    meta.Middleware,
		Responses:   make(map[int]any),
	}

	if op.ID == "" {
		op.ID = nameOf(handler)
	}

	// Always register header struct
	op.Header = new(H)

	// Register request body if B is not None
	if reflect.TypeOf((*B)(nil)).Elem() != noneType {
		op.Request = new(B)
	}

	// Register response with status from S, body from R (nil if None)
	var respBody any
	if reflect.TypeOf((*R)(nil)).Elem() != noneType {
		respBody = new(R)
	}
	op.Responses[GetStatus(new(S))] = respBody

	// Register error response schemas
	for _, err := range meta.Errors {
		if resp, ok := err.(HttpResponse); ok {
			op.Responses[resp.HttpResponse().Status] = err
		}
	}

	// Apply middleware chain
	chain := make([]httputil.Constructor, len(meta.Middleware))
	for i, fn := range meta.Middleware {
		chain[i] = httputil.Constructor(fn)
	}
	op.Handler = httputil.NewChain(chain...).Then(op.Handler)

	return op
}