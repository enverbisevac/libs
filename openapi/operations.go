package openapi

import "net/http"

func NewOperation[T, K, V any, R Success](
	handler OpenAPIHandleFunc[T, K, V, R],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(T)),
		WithRequest(new(K)),
		WithResponse(
			GetStatus(new(R)), new(V),
		),
	)
	return Handle(
		handler,
		args...,
	)
}

func NewOpNoBody[T any, R Success](
	handler OpNoBody[T, R],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(T)),
		WithResponse(
			GetStatus(new(R)), nil,
		),
	)
	return Handle(
		handler,
		args...,
	)
}

func NewOpNoBodyWithResponse[T, V any, R Success](
	handler OpNoBodyWithResponse[T, V, R],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(T)),
		WithResponse(
			GetStatus(new(R)), new(V),
		),
	)
	return Handle(
		handler,
		args...,
	)
}

func NewOpNoResponseBody[T, V any](
	handler OpNoResponseBody[T, V],
	options ...OperationFunc,
) *Operation {
	args := make([]OperationFunc, len(options))
	copy(args, options)
	args = append(
		args,
		WithHeader(new(T)),
		WithResponse(
			http.StatusNoContent, nil,
		),
	)
	return Handle(
		handler,
		args...,
	)
}

// func NewOperation[T, K, V any, R Success](
// 	handler OpenAPIHandleFunc[T, K, V, R],
// 	options ...OperationFunc,
// ) *Operation {
// 	cOp := Operation{
// 		Handler: handler,
// 	}

// 	for _, fn := range options {
// 		fn(&cOp)
// 	}

// 	cOp.Header = new(T)
// 	cOp.Request = new(K)

// 	var status R
// 	statusCode := http.StatusOK
// 	stat, ok := any(status).(HttpStatus)
// 	if ok {
// 		statusCode = stat.HttpStatus()
// 	}

// 	if statusCode != http.StatusNoContent {
// 		cOp.Responses = map[int]any{
// 			statusCode: new(V),
// 		}
// 	}

// 	for _, err := range cOp.Errors {
// 		resp, ok := err.(HttpResponse)
// 		if ok {
// 			cOp.Responses[resp.HttpResponse().Status] = err
// 		}
// 	}

// 	handlers := make([]httputil.Constructor, len(cOp.handlers))
// 	for i, fn := range cOp.handlers {
// 		handlers[i] = httputil.Constructor(fn)
// 	}

// 	cOp.Handler = httputil.NewChain(handlers...).Then(cOp.Handler)

// 	return &cOp
// }
