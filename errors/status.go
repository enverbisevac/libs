package errors

import (
	"errors"
	"fmt"
)

type Code string

const (
	CodeConflict           Code = "conflict"
	CodeInternal           Code = "internal"
	CodeInvalidArgument    Code = "invalid"
	CodeNotFound           Code = "not_found"
	CodeNotImplemented     Code = "not_implemented"
	CodeUnauthenticated    Code = "unauthenticated"
	CodeUnauthorized       Code = "unauthorized"
	CodePreconditionFailed Code = "precondition_failed"
	CodeAborted            Code = "aborted"
)

type Status struct {
	// Source error
	Err error `json:"source_error,omitempty"`

	// Machine-readable status code.
	Code Code `json:"code"`

	// Human-readable error message.
	Message string `json:"message"`

	// Payload
	Payload any `json:"detail,omitempty"`
}

// Unwrap status error and return source error.
func (e *Status) Unwrap() error {
	return e.Err
}

// Source sets the origin err and return error.
func (e *Status) Source(err error) *Status {
	e.Err = err
	return e
}

// Error implements the error interface.
func (e *Status) Error() string {
	return e.Message
}

func (e *Status) Detail(arg any) *Status {
	e.Payload = arg
	return e
}

// Http returns http status code mapped to error status code.
func (e *Status) Http() int {
	return HttpMap[e.Code]
}

// AsCode unwraps an error and returns its code.
// Non-application errors always return CodeInternal.
func AsCode(err error) Code {
	if err == nil {
		return ""
	}
	e := AsStatus(err)
	if e != nil {
		return e.Code
	}
	return CodeInternal
}

// Message unwraps an error and returns its message.
func Message(err error) string {
	if err == nil {
		return ""
	}
	e := AsStatus(err)
	if e != nil {
		return e.Message
	}
	return err.Error()
}

// Detail returns generic type stored in details.
func Detail[T any](err error) (detail *T) {
	if err == nil {
		return nil
	}
	e := AsStatus(err)
	if e != nil {
		v, ok := e.Payload.(T)
		if ok {
			return &v
		}
	}
	return nil
}

// AsStatus return err as Status rror.
func AsStatus(err error) (e *Status) {
	if err == nil {
		return nil
	}
	if errors.As(err, &e) {
		return
	}
	return
}

// Source read status error source.
func Source(err error) error {
	if e := AsStatus(err); e != nil {
		return e.Err
	}
	return err
}

// IsStatus checks if err is Status type.
func IsStatus(err error) bool {
	return errors.Is(err, &Status{})
}

// Http returns http status.
func HttpStatus(err error) int {
	return AsStatus(err).Http()
}

// Format is a helper function to return an Error with a given status and formatted message.
func Format(code Code, format string, args ...interface{}) *Status {
	msg := fmt.Sprintf(format, args...)
	newErr := &Status{
		Code:    code,
		Message: msg,
	}
	return newErr
}

// NotFound is a helper function to return an not found Error.
func NotFound(format string, args ...interface{}) *Status {
	return Format(CodeNotFound, format, args...)
}

// NotImplemented is a helper function to return an not found Error.
func NotImplemented(format string, args ...interface{}) *Status {
	return Format(CodeNotImplemented, format, args...)
}

// InvalidArgument is a helper function to return an invalid argument Error.
func InvalidArgument(format string, args ...interface{}) *Status {
	return Format(CodeInvalidArgument, format, args...)
}

// Internal is a helper function to return an internal Error.
func Internal(format string, args ...interface{}) *Status {
	return Format(CodeInternal, format, args...)
}

// Conflict is a helper function to return an conflict Error.
func Conflict(format string, args ...interface{}) *Status {
	return Format(CodeConflict, format, args...)
}

// PreconditionFailed is a helper function to return an precondition
// failed error.
func PreconditionFailed(format string, args ...interface{}) *Status {
	return Format(CodePreconditionFailed, format, args...)
}

// Aborted is a helper function to return aborted error status.
func Aborted(format string, args ...interface{}) *Status {
	return Format(CodeAborted, format, args...)
}

// Unauthenticated is a helper function to return unauthenticated error status.
func Unauthenticated(format string, args ...interface{}) *Status {
	return Format(CodeUnauthenticated, format, args...)
}

// Unauthorized is a helper function to return unauthorized error status.
func Unauthorized(format string, args ...interface{}) *Status {
	return Format(CodeUnauthorized, format, args...)
}

// IsNotFound checks if err is not found error.
func IsNotFound(err error) bool {
	return AsCode(err) == CodeNotFound
}

// IsNotImplemented checks if err is not implemented error.
func IsNotImplemented(err error) bool {
	return AsCode(err) == CodeNotImplemented
}

// IsConflict checks if err is conflict error.
func IsConflict(err error) bool {
	return AsCode(err) == CodeConflict
}

// IsInvalidArgument checks if err is invalid argument error.
func IsInvalidArgument(err error) bool {
	return AsCode(err) == CodeInvalidArgument
}

// IsInternal checks if err is internal error.
func IsInternal(err error) bool {
	return AsCode(err) == CodeInternal
}

// IsPreconditionFailed checks if err is precondition failed error.
func IsPreconditionFailed(err error) bool {
	return AsCode(err) == CodePreconditionFailed
}

// IsAborted checks if err is aborted error.
func IsAborted(err error) bool {
	return AsCode(err) == CodeAborted
}

// IsUnauthenticated checks if err is unauthenticated error.
func IsUnauthenticated(err error) bool {
	return AsCode(err) == CodeUnauthenticated
}

// IsUnauthorized checks if err is unauthorized error.
func IsUnauthorized(err error) bool {
	return AsCode(err) == CodeUnauthorized
}
