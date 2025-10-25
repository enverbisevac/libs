package errors

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestConflict(t *testing.T) {
	msg := "article 123 already exist"

	err := Conflict(msg)

	if !IsConflict(err) {
		t.Errorf("expected ConflictError, got: %T", err)
	}

	c, _ := AsConflict(err)

	if c.HttpResponse().Status != http.StatusConflict {
		t.Errorf("expected http status 409, got: %d", c.HttpResponse().Status)
	}

	if c.Error() != msg {
		t.Errorf("expected %s, got: %s", msg, c.Error())
	}
}

func TestConflictItem(t *testing.T) {
	err := Conflict("article ${%d} already exist", 123)

	if !IsConflict(err) {
		t.Errorf("expected ConflictError, got: %T", err)
	}

	c, _ := AsConflict(err)

	if c.HttpResponse().Status != http.StatusConflict {
		t.Errorf("expected http status 409, got: %d", c.HttpResponse().Status)
	}

	msg := fmt.Sprintf("article %d already exist", 123)

	if c.Error() != msg {
		t.Errorf("expected %s, got: %s", msg, c.Error())
	}

	if v, ok := c.Item.(int); !ok || v != 123 {
		t.Errorf("expected value %d, got: %d", 123, v)
	}
}

type mergeConflictError struct {
	ConflictError
	Base  string   `json:"base"`
	Head  string   `json:"head"`
	Files []string `json:"files"`
}

func (e *mergeConflictError) Error() string {
	return fmt.Sprintf("merge failed for base '%s' and head '%s'", e.Base, e.Head)
}

func TestConflictWithDetail(t *testing.T) {
	base := "main"
	head := "dev"

	err := &mergeConflictError{
		Base:  base,
		Head:  head,
		Files: []string{"test.txt"},
	}

	if !IsConflict(err) {
		t.Errorf("expected conflict error, got: %s", err)
	}

	var cerr *mergeConflictError
	if ok := errors.As(err, &cerr); !ok || len(cerr.Files) == 0 {
		t.Errorf("expected length of files should be 1, got: %d", len(cerr.Files))
	}

	if cerr.HttpResponse().Status != http.StatusConflict {
		t.Errorf("expected http status 409, got: %d", cerr.HttpResponse().Status)
	}

	if cerr.Files[0] != "test.txt" {
		t.Errorf("expected test.txt, got: %s", cerr.Files[0])
	}
}

func TestNotFound(t *testing.T) {
	err := NotFound("article 123 not found")

	if !IsNotFound(err) {
		t.Errorf("expected not found error, got: %s", err)
	}

	c, ok := AsNotFound(err)

	if !ok {
		t.Errorf("expected not found error, got: %v", err)
	}

	if c.HttpResponse().Status != http.StatusNotFound {
		t.Errorf("expected http status 400, got: %d", c.HttpResponse().Status)
	}

	if c.Error() != "article 123 not found" {
		t.Errorf("expected article 123 not found, got: %s", c.Error())
	}
}

type pathNotFoundError struct {
	NotFoundError
	Path string `json:"path"`
}

func (e *pathNotFoundError) Error() string {
	return fmt.Sprintf("path %s not found", e.Path)
}

func (e *pathNotFoundError) MarshalJSON() ([]byte, error) {
	type NotFoundErrorAlias NotFoundError
	aux := struct {
		NotFoundErrorAlias
		Path string `json:"path"`
	}{
		NotFoundErrorAlias: NotFoundErrorAlias{
			Base: Base{
				Msg: e.Error(),
			},
		},
		Path: e.Path,
	}

	return json.Marshal(aux)
}

func TestNotFoundWithDetail(t *testing.T) {
	path := "/users"

	err := &pathNotFoundError{
		Path: path,
	}

	if !IsNotFound(err) {
		t.Errorf("expected path not found error, got: %s", err)
	}

	var cerr *pathNotFoundError
	if ok := errors.As(err, &cerr); !ok || len(cerr.Path) == 0 {
		t.Errorf("expected length of path should be greater than 0, got: %d", len(cerr.Path))
	}

	if cerr.HttpResponse().Status != http.StatusNotFound {
		t.Errorf("expected http status 404, got: %d", cerr.HttpResponse().Status)
	}

	if cerr.Path != "/users" {
		t.Errorf("expected /users, got: %s", cerr.Path)
	}
}

func TestNotFoundJSON(t *testing.T) {
	path := "/users"

	err := &pathNotFoundError{
		Path: path,
	}

	if !IsNotFound(err) {
		t.Errorf("expected path not found error, got: %s", err)
	}

	var cerr *pathNotFoundError
	if ok := errors.As(err, &cerr); !ok || len(cerr.Path) == 0 {
		t.Errorf("expected length of path should be greater than 0, got: %d", len(cerr.Path))
	}

	buff := bytes.Buffer{}

	if json.NewEncoder(&buff).Encode(cerr) != nil {
		t.Errorf("expected nil error, got: %v", cerr)
	}

	if buff.Len() == 0 {
		t.Errorf("expected buff length > 0, got: %v", buff.Len())
	}
}

func TestValidation(t *testing.T) {
	err := Validation("article validation error").
		AddError(errors.New("title is required field")).
		AddError(Validation("status must be one of the values [draft, published]"))

	if !IsValidation(err) {
		t.Errorf("expected validation error, got: %s", err)
	}

	c, ok := AsValidation(err)

	if !ok {
		t.Errorf("expected validation error, got: %v", err)
	}

	if c.HttpResponse().Status != http.StatusBadRequest {
		t.Errorf("expected http status 400, got: %d", c.HttpResponse().Status)
	}

	if c.Error() != "article validation error" {
		t.Errorf("expected article validation error, got: %s", c.Error())
	}

	if len(c.Errors) == 0 {
		t.Errorf("expected 2 argument errors, got: %d", len(c.Errors))
	}
}

func TestConflictError_HttpStatus(t *testing.T) {
	type fields struct {
		Item any
		Msg  string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ConflictError{
				Item: tt.fields.Item,
				Base: Base{
					Msg: tt.fields.Msg,
				},
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("ConflictError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotFoundError_HttpStatus(t *testing.T) {
	type fields struct {
		Item any
		Msg  string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &NotFoundError{
				Item: tt.fields.Item,
				Base: Base{
					Msg: tt.fields.Msg,
				},
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("NotFoundError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInternalError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg        string
		Err        error
		Stacktrace []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &InternalError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
				Err:        tt.fields.Err,
				Stacktrace: tt.fields.Stacktrace,
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("InternalError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPreconditionFailedError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg string
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &PreconditionFailedError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
				Err: tt.fields.Err,
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("PreconditionFailedError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidationError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg    string
		Errors []error
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ValidationError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
				Errors: tt.fields.Errors,
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("ValidationError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotImplementedError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &NotImplementedError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("NotImplementedError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnauthenticatedError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &UnauthenticatedError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("UnauthenticatedError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnauthorizedError_HttpStatus(t *testing.T) {
	type fields struct {
		Msg string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &UnauthorizedError{
				Base: Base{
					Msg: tt.fields.Msg,
				},
			}
			if got := e.HttpResponse().Status; got != tt.want {
				t.Errorf("UnauthorizedError.HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetails(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		err          error
		wantErrValue error
		wantErr      bool
	}{
		{
			name:         "test validation error",
			err:          Validation("validation error").AddError(errors.New("field is required")),
			wantErrValue: errors.New("validation error\n\n\t - field is required"),
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := Details(tt.err)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Details() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Details() succeeded unexpectedly")
			}
		})
	}
}
