package openapi

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/enverbisevac/libs/errors"
	"github.com/enverbisevac/libs/httputil"
	"github.com/go-chi/chi/v5"
	"github.com/swaggest/openapi-go"
	"github.com/swaggest/openapi-go/openapi3"
	"gopkg.in/yaml.v3"
)

type OperationContext interface {
	OperationContext(
		reflector *openapi3.Reflector,
		method string,
		route string,
	) (openapi.OperationContext, error)
}

func GenerateOAS(reflector *openapi3.Reflector, router chi.Router) error {
	walkFunc := func(method string, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
		op, ok := handler.(OperationContext)
		if ok {
			opCtx, err := op.OperationContext(reflector, method, strings.TrimRight(route, "/"))
			if err != nil {
				panic(err)
			}
			if err := reflector.AddOperation(opCtx); err != nil {
				panic(err)
			}
		}
		return nil
	}

	if err := chi.Walk(router, walkFunc); err != nil {
		fmt.Printf("Logging err: %s\n", err.Error())
	}
	return nil
}

type Operation struct {
	http.Handler
	handlers []func(http.Handler) http.Handler

	ID          string
	Summary     string
	Description string
	Tags        []string

	Path      any
	Header    any
	Request   any
	Responses map[int]any

	Success  int
	Errors   []error
	Security []string
}

func (o *Operation) OperationContext(
	reflector *openapi3.Reflector,
	method string,
	route string,
) (openapi.OperationContext, error) {
	op, err := reflector.NewOperationContext(method, route)
	if err != nil {
		return nil, fmt.Errorf("failed to map OperationContext: %w", err)
	}

	op.SetID(o.ID)
	op.SetSummary(o.Summary)
	op.SetDescription(o.Description)
	op.SetTags(o.Tags...)

	if o.Path != nil {
		op.AddReqStructure(o.Path)
	}

	if o.Header != nil {
		op.AddReqStructure(o.Header)
	}

	if o.Request != nil {
		op.AddReqStructure(o.Request)
	}

	for status, object := range o.Responses {
		op.AddRespStructure(object, openapi.WithHTTPStatus(status))
	}

	for _, sec := range o.Security {
		op.AddSecurity(sec)
	}

	return op, nil
}

type OperationFunc func(*Operation)

func WithID(id string) OperationFunc {
	return func(o *Operation) {
		o.ID = id
	}
}

func WithSummary(summary string) OperationFunc {
	return func(o *Operation) {
		o.Summary = summary
	}
}

func WithDescription(description string) OperationFunc {
	return func(o *Operation) {
		o.Description = description
	}
}

func WithTags(tags ...string) OperationFunc {
	return func(o *Operation) {
		o.Tags = tags
	}
}

func WithSuccess(status int) OperationFunc {
	return func(o *Operation) {
		o.Success = status
	}
}

func WithErrors(errors ...error) OperationFunc {
	return func(o *Operation) {
		o.Errors = append(o.Errors, errors...)
	}
}

func WithSecurity(methods ...string) OperationFunc {
	return func(o *Operation) {
		o.Security = append(o.Security, methods...)
	}
}

func WithHandlers(handlers ...func(http.Handler) http.Handler) OperationFunc {
	return func(o *Operation) {
		o.handlers = append(o.handlers, handlers...)
	}
}

type HttpRequest interface {
	SetRequest(req *http.Request)
}

type Encoder interface {
	Encode(v any) error
}

type Decoder interface {
	Decode(v any) error
}

type Request[T any, K any, V any] struct {
	Path   T
	Header K
	Body   V
}

type OpenAPIHandleFunc[T, K, V, R any] func(Context, Request[T, K, V]) (R, error)

func (f OpenAPIHandleFunc[T, K, V, R]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var path T
	var header K
	var body V
	contentType := r.Header.Get("Content-Type")
	accept := r.Header.Get("Accept")

	var decoder Decoder

	switch contentType {
	default:
		fallthrough
	case "application/json":
		decoder = json.NewDecoder(r.Body)
	case "application/yaml":
		decoder = yaml.NewDecoder(r.Body)
	}

	var encoder Encoder

	switch accept {
	case "application/json":
		encoder = json.NewEncoder(w)
	case "application/xml":
		encoder = xml.NewEncoder(w)
	case "application/yaml":
		encoder = yaml.NewEncoder(w)
	default:
		encoder = json.NewEncoder(w)
	}

	err := decoder.Decode(&path)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	err = decoder.Decode(&header)
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}

	err = httputil.Decode(r, chi.URLParam, &body)
	if err != nil && !errors.Is(err, io.EOF) {
		errors.Response(encoder, w, err)
		return
	}

	v, err := f(Context{
		Request:  r,
		Response: w,
	}, Request[T, K, V]{
		Path:   path,
		Header: header,
		Body:   body,
	})
	if err != nil {
		errors.Response(encoder, w, err)
		return
	}
	statusValue := r.Context().Value(ctxStatus{})
	status, ok := statusValue.(int)
	if !ok {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if status != http.StatusNoContent {
		encoder.Encode(v)
	}
}

type ctxStatus struct{}

type HttpResponse interface {
	HttpResponse() errors.HttpResponse
}

func NewOperation[T, K, V, R any](
	handler OpenAPIHandleFunc[T, K, V, R],
	options ...OperationFunc,
) *Operation {
	cOp := Operation{}

	for _, fn := range options {
		fn(&cOp)
	}

	cOp.Path = new(T)
	cOp.Header = new(K)
	cOp.Request = new(V)
	cOp.Responses = map[int]any{
		cOp.Success: new(R),
	}

	for _, err := range cOp.Errors {
		resp, ok := err.(HttpResponse)
		if ok {
			cOp.Responses[resp.HttpResponse().Status] = err
		}
	}

	cOp.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), ctxStatus{}, cOp.Success)
		r = r.WithContext(ctx)
		handler.ServeHTTP(w, r)
	})

	handlers := make([]httputil.Constructor, len(cOp.handlers))
	for i, fn := range cOp.handlers {
		handlers[i] = httputil.Constructor(fn)
	}

	cOp.Handler = httputil.NewChain(handlers...).Then(cOp.Handler)

	return &cOp
}

type Context struct {
	Request  *http.Request
	Response interface {
		Header() http.Header
	}
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
