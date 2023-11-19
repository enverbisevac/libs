package errors

import (
	"errors"
	"fmt"
)

type Status string

const (
	StatusConflict           Status = "conflict"
	StatusInternal           Status = "internal"
	StatusInvalidArgument    Status = "invalid"
	StatusNotFound           Status = "not_found"
	StatusNotImplemented     Status = "not_implemented"
	StatusUnauthenticated    Status = "unauthenticated"
	StatusUnauthorized       Status = "unauthorized"
	StatusPreconditionFailed Status = "precondition_failed"
	StatusAborted            Status = "aborted"
)

type Error struct {
	// Source error
	Err error

	// Machine-readable status code.
	Status Status

	// Human-readable error message.
	Message string

	// Details
	details []any
}

// Unwrap error and return source error.
func (e *Error) Unwrap() error {
	return e.Err
}

// Source sets the origin err and return error.
func (e *Error) Source(err error) *Error {
	e.Err = err
	return e
}

// Error implements the error interface.
func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Details(args ...any) *Error {
	if e.details == nil {
		e.details = append(e.details, args...)
	}
	return e
}

// Http returns http status based on error status.
func (e *Error) Http() int {
	return HttpMap[e.Status]
}

// AsStatus unwraps an error and returns its code.
// Non-application errors always return StatusInternal.
func AsStatus(err error) Status {
	if err == nil {
		return ""
	}
	e := AsError(err)
	if e != nil {
		return e.Status
	}
	return StatusInternal
}

// Message unwraps an error and returns its message.
func Message(err error) string {
	if err == nil {
		return ""
	}
	e := AsError(err)
	if e != nil {
		return e.Message
	}
	return err.Error()
}

// Details unwraps an error and returns its details.
func Details(err error) []any {
	if err == nil {
		return nil
	}
	e := AsError(err)
	if e != nil {
		return e.details
	}
	return nil
}

// Detail returns generic type stored in details.
func Detail[T any](err error) (detail *T) {
	if err == nil {
		return nil
	}
	e := AsError(err)
	if e != nil {
		for _, detail := range e.details {
			v, ok := detail.(T)
			if ok {
				return &v
			}
		}
	}
	return nil
}

// AsError return err as Error.
func AsError(err error) (e *Error) {
	if err == nil {
		return nil
	}
	if errors.As(err, &e) {
		return
	}
	return
}

// IsStatus checks if err is Error type.
func IsStatus(err error) bool {
	return errors.Is(err, &Error{})
}

// Http returns http status.
func HttpStatus(err error) int {
	return AsError(err).Http()
}

// Format is a helper function to return an Error with a given status and formatted message.
func Format(code Status, format string, args ...interface{}) *Error {
	msg := fmt.Sprintf(format, args...)
	newErr := &Error{
		Status:  code,
		Message: msg,
	}
	return newErr
}

// NotFound is a helper function to return an not found Error.
func NotFound(format string, args ...interface{}) *Error {
	return Format(StatusNotFound, format, args...)
}

// InvalidArgument is a helper function to return an invalid argument Error.
func InvalidArgument(format string, args ...interface{}) *Error {
	return Format(StatusInvalidArgument, format, args...)
}

// Internal is a helper function to return an internal Error.
func Internal(format string, args ...interface{}) *Error {
	return Format(StatusInternal, format, args...)
}

// Conflict is a helper function to return an conflict Error.
func Conflict(format string, args ...interface{}) *Error {
	return Format(StatusConflict, format, args...)
}

// PreconditionFailed is a helper function to return an precondition
// failed error.
func PreconditionFailed(format string, args ...interface{}) *Error {
	return Format(StatusPreconditionFailed, format, args...)
}

// Aborted is a helper function to return aborted error status.
func Aborted(format string, args ...interface{}) *Error {
	return Format(StatusAborted, format, args...)
}

// Unauthenticated is a helper function to return unauthenticated error status.
func Unauthenticated(format string, args ...interface{}) *Error {
	return Format(StatusUnauthenticated, format, args...)
}

// Unauthorized is a helper function to return unauthorized error status.
func Unauthorized(format string, args ...interface{}) *Error {
	return Format(StatusUnauthorized, format, args...)
}

// IsNotFound checks if err is not found error.
func IsNotFound(err error) bool {
	return AsStatus(err) == StatusNotFound
}

// IsConflict checks if err is conflict error.
func IsConflict(err error) bool {
	return AsStatus(err) == StatusConflict
}

// IsInvalidArgument checks if err is invalid argument error.
func IsInvalidArgument(err error) bool {
	return AsStatus(err) == StatusInvalidArgument
}

// IsInternal checks if err is internal error.
func IsInternal(err error) bool {
	return AsStatus(err) == StatusInternal
}

// IsPreconditionFailed checks if err is precondition failed error.
func IsPreconditionFailed(err error) bool {
	return AsStatus(err) == StatusPreconditionFailed
}

// IsAborted checks if err is aborted error.
func IsAborted(err error) bool {
	return AsStatus(err) == StatusAborted
}

// IsUnauthenticated checks if err is unauthenticated error.
func IsUnauthenticated(err error) bool {
	return AsStatus(err) == StatusUnauthenticated
}

// IsUnauthorized checks if err is unauthorized error.
func IsUnauthorized(err error) bool {
	return AsStatus(err) == StatusUnauthorized
}
