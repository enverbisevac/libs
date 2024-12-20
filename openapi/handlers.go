package openapi

import (
	"net/http"

	"github.com/enverbisevac/libs/errors"
	"github.com/enverbisevac/libs/httputil"
	"github.com/go-chi/chi/v5"
)

type OpenAPIHandleFunc[T, K, V any, R Success] func(ctx Context, in Request[T, K], out *V) (*R, error)

func (f OpenAPIHandleFunc[T, K, V, R]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header T
	var body K

	encoder, decoder := GetEncDec(w, r)

	err := DecodeRequest(r, decoder, &body, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	var value V
	status, err := f(Context{
		Request: r,
	}, Request[T, K]{
		Header: header,
		Body:   body,
	}, &value)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, &value, status)
}

type OpNoBody[T any, R Success] func(ctx Context, in T) (*R, error)

func (f OpNoBody[T, R]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header T

	encoder, _ := GetEncDec(w, r)

	err := httputil.Decode(r, chi.URLParam, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	status, err := f(Context{
		Request: r,
	}, header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, (*T)(nil), status)
}
