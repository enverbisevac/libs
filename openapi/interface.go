package openapi

import "github.com/enverbisevac/libs/errors"

type Encoder interface {
	Encode(v any) error
}

type Decoder interface {
	Decode(v any) error
}

type HttpStatus interface {
	HttpStatus() int
}

type HttpResponse interface {
	HttpResponse() errors.HttpResponse
}
