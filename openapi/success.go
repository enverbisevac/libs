package openapi

import "net/http"

type OK struct {
	Header http.Header
}

func (OK) HttpStatus() int {
	return http.StatusOK
}

type Created struct {
	Header http.Header
}

func (Created) HttpStatus() int {
	return http.StatusCreated
}

type Accepted struct {
	Header http.Header
}

func (Accepted) HttpStatus() int {
	return http.StatusAccepted
}

type NoContent struct {
	Header http.Header
}

func (NoContent) HttpStatus() int {
	return http.StatusNoContent
}

type Success interface {
	OK | Created | Accepted | NoContent
}
