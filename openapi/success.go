package openapi

import "net/http"

type OK struct{}

func (OK) HttpStatus() int {
	return http.StatusOK
}

type Created struct{}

func (Created) HttpStatus() int {
	return http.StatusCreated
}

type Accepted struct{}

func (Accepted) HttpStatus() int {
	return http.StatusAccepted
}

type NoContent struct{}

func (NoContent) HttpStatus() int {
	return http.StatusNoContent
}

type Success interface {
	OK | Created | Accepted | NoContent
}