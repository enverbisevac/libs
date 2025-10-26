package errors

import (
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
		err     error
		wantErr error
	}{
		{
			name:    "test validation error",
			err:     Validation("validation error"),
			wantErr: errors.New("validation error"),
		},
		{
			name:    "test validation multiple errors",
			err:     Validation("validation error").AddError(errors.New("field is required")),
			wantErr: errors.New("validation error\n\n\t - field is required\n"),
		},
		{
			name:    "test validation wrap error",
			err:     fmt.Errorf("in Test: %w", Validation("validation error").AddError(errors.New("field is required"))),
			wantErr: errors.New("validation error\n\n\t - field is required\n"),
		},
		{
			name:    "test conflict error",
			err:     Conflict("resource conflict"),
			wantErr: errors.New("resource conflict"),
		},
		{
			name:    "test conflict multiple errors",
			err:     Conflict("resource conflict").AddError(errors.New("resource id already exists")),
			wantErr: errors.New("resource conflict\n\n\t - resource id already exists\n"),
		},
		{
			name:    "test conflict wrap error",
			err:     fmt.Errorf("in Test: %w", Conflict("resource conflict").AddError(errors.New("resource id already exists"))),
			wantErr: errors.New("resource conflict\n\n\t - resource id already exists\n"),
		},
		{
			name:    "not found error",
			err:     NotFound("resource not found"),
			wantErr: errors.New("resource not found"),
		},
		{
			name:    "not found wrap error",
			err:     fmt.Errorf("in Test: %w", NotFound("resource not found")),
			wantErr: errors.New("resource not found"),
		},
		{
			name:    "internal error",
			err:     Internal(errors.New("db connection failed"), "internal server error"),
			wantErr: errors.New("internal server error\n\nCaused by:\n\tdb connection failed\n"),
		},
		{
			name:    "internal wrap error",
			err:     fmt.Errorf("in Test: %w", Internal(errors.New("db connection failed"), "internal server error")),
			wantErr: errors.New("internal server error\n\nCaused by:\n\tdb connection failed\n"),
		},
		{
			name:    "precondition failed error",
			err:     PreconditionFailed("precondition failed"),
			wantErr: errors.New("precondition failed"),
		},
		{
			name:    "precondition failed set error",
			err:     PreconditionFailed("precondition failed").SetErr(errors.New("etag mismatch")),
			wantErr: errors.New("precondition failed\n\nCaused by:\n\tetag mismatch\n"),
		},
		{
			name:    "precondition failed wrap error",
			err:     fmt.Errorf("in Test: %w", PreconditionFailed("precondition failed")),
			wantErr: errors.New("precondition failed"),
		},
		{
			name:    "validation error",
			err:     Validation("validation error"),
			wantErr: errors.New("validation error"),
		},
		{
			name: "validation error multiple errors",
			err: Validation("validation error").
				AddError(errors.New("title is required field")).
				AddError(Validation("status must be one of the values [draft, published]")),
			wantErr: errors.New("validation error\n\n\t - title is required field\n\t - status must be one of the values [draft, published]\n"),
		},
		{
			name:    "validation wrap error",
			err:     fmt.Errorf("in Test: %w", Validation("validation error")),
			wantErr: errors.New("validation error"),
		},
		{
			name:    "not implemented error",
			err:     NotImplemented("feature not implemented"),
			wantErr: errors.New("feature not implemented"),
		},
		{
			name:    "not implemented wrap error",
			err:     fmt.Errorf("in Test: %w", NotImplemented("feature not implemented")),
			wantErr: errors.New("feature not implemented"),
		},
		{
			name:    "unauthenticated error",
			err:     Unauthenticated("user not authenticated"),
			wantErr: errors.New("user not authenticated"),
		},
		{
			name:    "unauthenticated wrap error",
			err:     fmt.Errorf("in Test: %w", Unauthenticated("user not authenticated")),
			wantErr: errors.New("user not authenticated"),
		},
		{
			name:    "unauthorized error",
			err:     Unauthorized("user not authorized"),
			wantErr: errors.New("user not authorized"),
		},
		{
			name:    "unauthorized wrap error",
			err:     fmt.Errorf("in Test: %w", Unauthorized("user not authorized")),
			wantErr: errors.New("user not authorized"),
		},
		{
			name:    "other error",
			err:     errors.New("some other error"),
			wantErr: errors.New("some other error"),
		},
		{
			name:    "test nil error",
			err:     nil,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := Details(tt.err)
			if (gotErr == nil) != (tt.wantErr == nil) {
				t.Errorf("Details() = %v, want %v", gotErr, tt.wantErr)
				return
			}
			if gotErr != nil && gotErr.Error() != tt.wantErr.Error() {
				t.Errorf("Details() = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}
