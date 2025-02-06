package openapi

import (
	"net/http"

	"github.com/enverbisevac/libs/errors"
	"github.com/enverbisevac/libs/httputil"
	"github.com/go-chi/chi/v5"
)

type HandleFunc1[H any, S Success] func(ctx Context, in H) (*S, error)

func (f HandleFunc1[H, S]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header H

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

	EncodeResponse(w, encoder, nil, status)
}

func NewOp1[H any, S Success](
	handler HandleFunc1[H, S],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(H)),
		WithResponse(
			GetStatus(new(S)), nil,
		),
	)
	return Handle(
		handler,
		args...,
	)
}

type HandleFunc2[H, B any, S Success] func(ctx Context, in Request[H, B]) (*S, error)

func (f HandleFunc2[H, B, S]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header H
	var body B

	encoder, _ := GetEncDec(w, r)

	err := httputil.Decode(r, chi.URLParam, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	status, err := f(Context{
		Request: r,
	}, Request[H, B]{
		Header: header,
		Body:   body,
	})
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, nil, status)
}

func NewOp2[H, B any, S Success](
	handler HandleFunc2[H, B, S],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(H)),
		WithRequest(new(B)),
		WithResponse(
			GetStatus(new(S)), nil,
		),
	)
	return Handle(
		handler,
		args...,
	)
}

type HandleFunc3[H, B, R any, S Success] func(ctx Context, in Request[H, B], out *R) (*S, error)

func (f HandleFunc3[H, B, R, S]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header H
	var body B

	encoder, decoder := GetEncDec(w, r)

	err := DecodeRequest(r, decoder, &body, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	var value R
	status, err := f(Context{
		Request: r,
	}, Request[H, B]{
		Header: header,
		Body:   body,
	}, &value)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, &value, status)
}

// NewOp3 define new operation with
// H path, query and header fields
// B request Body
// R response Body
// S if no err return from handler then return success
func NewOp3[H, B, R any, S Success](
	handler HandleFunc3[H, B, R, S],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(H)),
		WithRequest(new(B)),
		WithResponse(
			GetStatus(new(S)), new(R),
		),
	)
	return Handle(
		handler,
		args...,
	)
}

type HandleFunc4[H, V any, S Success] func(ctx Context, in H, out *V) (*S, error)

func (f HandleFunc4[H, V, R]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header H

	encoder, _ := GetEncDec(w, r)

	err := httputil.Decode(r, chi.URLParam, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	var value V
	status, err := f(Context{
		Request: r,
	}, header, &value)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, &value, status)
}

// NewOp4 define new operation with
// H path, query and header fields
// R response Body
// S if no err return from handler then return success
// This usable in GET requests.
func NewOp4[H, R any, S Success](
	handler HandleFunc4[H, R, S],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(H)),
		WithResponse(
			GetStatus(new(S)), new(R),
		),
	)
	return Handle(
		handler,
		args...,
	)
}

type HandleFunc5[H, B any] func(ctx Context, in Request[H, B]) (*NoContent, error)

func (f HandleFunc5[H, B]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var header H
	var body B

	encoder, decoder := GetEncDec(w, r)

	err := DecodeRequest(r, decoder, &body, &header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	status, err := f(Context{
		Request: r,
	}, Request[H, B]{
		Header: header,
		Body:   body,
	})
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	EncodeResponse(w, encoder, nil, status)
}

// NewOp5 define new operation with
// H path, query and header fields
// R response Body
// S if no err return from handler then return success
// This usable in GET requests.
func NewOp5[H, B any](
	handler HandleFunc5[H, B],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(H)),
		WithRequest(new(B)),
		WithResponse(
			GetStatus(&NoContent{}), nil,
		),
	)
	return Handle(
		handler,
		args...,
	)
}

func Security(operation *Operation, s ...string) *Operation {
	operation.Security = s
	return operation
}

func Handlers(operation *Operation, fn ...func(http.Handler) http.Handler) *Operation {
	operation.handlers = fn
	return operation
}

func GetHandler[H, V any, S Success](
	handler HandleFunc4[H, V, S],
	id string,
	summary string,
	tags []string,
) *Operation {
	return NewOp4(
		handler,
		WithID(id),
		WithSummary(summary),
		WithTags(tags...),
		WithErrors(
			new(errors.UnauthenticatedError),
			new(errors.UnauthorizedError),
			new(errors.NotFoundError),
			new(errors.InternalError),
		),
	)
}

func PutHandler[H, B, V any, S Success](
	handler HandleFunc3[H, B, V, S],
	id string,
	summary string,
	tags []string,
) *Operation {
	return NewOp3(
		handler,
		WithID(id),
		WithSummary(summary),
		WithTags(tags...),
		WithErrors(
			new(errors.ValidationError),
			new(errors.UnauthenticatedError),
			new(errors.UnauthorizedError),
			new(errors.NotFoundError),
			new(errors.InternalError),
		),
	)
}

func DeleteHandler[T any, S Success](
	handler HandleFunc1[T, S],
	id string,
	summary string,
	tags []string,
) *Operation {
	return NewOp1(
		handler,
		WithID(id),
		WithSummary(summary),
		WithTags(tags...),
		WithErrors(
			new(errors.UnauthenticatedError),
			new(errors.UnauthorizedError),
			new(errors.NotFoundError),
			new(errors.InternalError),
		),
	)
}
