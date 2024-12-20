package openapi

import (
	"fmt"
	"net/http"

	"github.com/swaggest/openapi-go"
	"github.com/swaggest/openapi-go/openapi3"
)

type Operation struct {
	http.Handler
	handlers []func(http.Handler) http.Handler

	ID          string
	Summary     string
	Description string
	Tags        []string

	Header    any
	Request   any
	Responses map[int]any

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

func WithHeader(object any) OperationFunc {
	return func(o *Operation) {
		o.Header = object
	}
}

func WithRequest(object any) OperationFunc {
	return func(o *Operation) {
		o.Request = object
	}
}

func WithResponse(status int, object any) OperationFunc {
	return func(o *Operation) {
		if o.Responses == nil {
			o.Responses = make(map[int]any)
		}
		o.Responses[status] = object
	}
}

type Request[T any, V any] struct {
	Header T
	Body   V
}
