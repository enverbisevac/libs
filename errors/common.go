package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

type Base struct {
	// Msg contains user friendly error.
	Msg       string    `json:"message"`
	Status    int       `json:"status,omitempty"`
	TraceID   string    `json:"trace_id,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

func NewBase(status int, format string, args ...any) Base {
	return Base{
		Msg:       fmt.Sprintf(format, args...),
		Status:    status,
		Timestamp: time.Now(),
	}
}

func (b *Base) SetTraceID(id string) {
	b.TraceID = id
}

func (b *Base) GetBase() *Base {
	return b
}

type tracer interface {
	SetTraceID(id string)
	Error() string
}

func TraceID(id string, t error) error {
	err, ok := t.(tracer)
	if ok {
		err.SetTraceID(id)
	}

	return t
}

// ConflictError holds fields for conflict error.
type ConflictError struct {
	Base
	// Item is a conflicting resource.
	Item   any               `json:"item,omitempty"`
	Errors MarshalableErrors `json:"errors"`
}

// Conflict is a helper function to return an ConflictError.
// In format argument Item can be specified with ${} specifier
// for example:
//   - errors.Conflict("article ${123} already exist")
//   - errors.Conflict("article ${%d} already exist", 123)
//   - errors.Conflict("group %s and article ${%d} already exist", 1, 123)
//
// the value of Item in ConflictError will be set to 123.
func Conflict(format string, args ...any) *ConflictError {
	format, item := parse(format, args...)

	return &ConflictError{
		Item: item,
		Base: NewBase((*ConflictError).HttpStatus(nil), format, args...),
	}
}

// IsConflict checks if err is conflict error.
func IsConflict(err error) bool {
	return errors.Is(err, &ConflictError{})
}

// AsConflict return err as ConflictError or nil if it is not
// successfull.
func AsConflict(err error) (cerr *ConflictError, b bool) {
	if errors.As(err, &cerr) {
		return cerr, true
	}

	return nil, false
}

// Error interface method.
func (e *ConflictError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	if e.Item != nil {
		return fmt.Sprintf("%v already exist", e.Item)
	}
	return "resource already exist"
}

// HttpStatus returns http status for ConflictError.
func (e *ConflictError) HttpStatus() int {
	return http.StatusConflict
}

func (e *ConflictError) AddError(err error) *ConflictError {
	e.Errors = append(e.Errors, err)
	return e
}

// Is checks if err is ConflictError.
func (e *ConflictError) Is(t error) bool {
	_, ok := t.(*ConflictError)
	return ok
}

type NotFoundError struct {
	Base
	Item any `json:"item,omitempty"`
}

// NotFound is a helper function to return an NotFoundError.
// In format argument Item can be specified with ${} specifier
// for example:
//   - errors.NotFound("article ${123} already exist")
//   - errors.NotFound("article ${%d} already exist", 123)
//   - errors.NotFound("group %s and article ${%d} already exist", 1, 123)
//
// the value of Item in NotFoundError will be set to 123.
func NotFound(format string, args ...any) *NotFoundError {
	format, item := parse(format, args...)
	return &NotFoundError{
		Item: item,
		Base: NewBase((*NotFoundError).HttpStatus(nil), format, args...),
	}
}

// IsNotFound checks if err is not found error.
func IsNotFound(err error) bool {
	return errors.Is(err, &NotFoundError{})
}

// AsNotFound return err as NotFoundError or nil if it is not
// successfull.
func AsNotFound(err error) (nerr *NotFoundError, b bool) {
	if errors.As(err, &nerr) {
		return nerr, true
	}

	return nil, false
}

// Error interface method.
func (e *NotFoundError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	if e.Item != nil {
		return fmt.Sprintf("%v not found", e.Item)
	}
	return "resource not found"
}

// Is returns http status for NotFoundError
func (e *NotFoundError) HttpStatus() int {
	return http.StatusNotFound
}

func (e *NotFoundError) Is(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}

type InternalError struct {
	Base
	Err        error           `json:"-"`
	Stacktrace json.RawMessage `json:"-"`
}

// Internal is a helper function to return an internal Error.
func Internal(err error, format string, args ...any) *InternalError {
	return &InternalError{
		Base:       NewBase((*InternalError).HttpStatus(nil), format, args...),
		Err:        err,
		Stacktrace: debug.Stack(),
	}
}

func AsInternal(err error) (ierr *InternalError, b bool) {
	if errors.As(err, &ierr) {
		return ierr, true
	}

	return nil, false
}

// IsInternal checks if err is internal error.
func IsInternal(err error) bool {
	return errors.Is(err, &InternalError{})
}

func (e *InternalError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	if e.Err != nil {
		return fmt.Sprintf("internal server error: %s", e.Err)
	}
	return "internal server error"
}

func (e *InternalError) HttpStatus() int {
	return http.StatusInternalServerError
}

type PreconditionFailedError struct {
	Base
	Err error
}

// Internal is a helper function to return an internal Error.
func PreconditionFailed(format string, args ...interface{}) *PreconditionFailedError {
	return &PreconditionFailedError{
		Base: NewBase((*PreconditionFailedError).HttpStatus(nil), format, args...),
	}
}

// IsInternal checks if err is internal error.
func IsPreconditionFailed(err error) bool {
	return errors.Is(err, &PreconditionFailedError{})
}

func (e *PreconditionFailedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "precondition failed error"
}

func (e *PreconditionFailedError) HttpStatus() int {
	return http.StatusPreconditionFailed
}

func (e *PreconditionFailedError) SetErr(err error) *PreconditionFailedError {
	e.Err = err
	return e
}

type MarshalableErrors []error

func (me MarshalableErrors) MarshalJSON() ([]byte, error) {
	data := []byte("[")
	for i, err := range me {
		if i != 0 {
			data = append(data, ',')
		}
		errstr := strings.ReplaceAll(err.Error(), "\n", " or ")
		j, err := json.Marshal(errstr)
		if err != nil {
			return nil, err
		}

		data = append(data, j...)
	}
	data = append(data, ']')

	return data, nil
}

type ValidationError struct {
	Base
	Errors MarshalableErrors `json:"errors,omitempty"`
}

// Validation is a helper function to return an invalid argument Error.
func Validation(format string, args ...any) *ValidationError {
	return &ValidationError{
		Base: NewBase((*ValidationError).HttpStatus(nil), format, args...),
	}
}

// IsValidation checks if err is invalid argument error.
func IsValidation(err error) bool {
	return errors.Is(err, &ValidationError{})
}

func AsValidation(err error) (verr *ValidationError, b bool) {
	if errors.As(err, &verr) {
		return verr, true
	}

	return nil, false
}

func (e *ValidationError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "validation error"
}

func (e *ValidationError) HttpStatus() int {
	return http.StatusBadRequest
}

func (e *ValidationError) AddError(err error) *ValidationError {
	e.Errors = append(e.Errors, err)
	return e
}

func (e *ValidationError) Is(err error) bool {
	_, ok := err.(*ValidationError)
	return ok
}

type NotImplementedError struct {
	Base
}

// InvalidArgument is a helper function to return an invalid argument Error.
func NotImplemented(format string, args ...any) *NotImplementedError {
	return &NotImplementedError{
		Base: NewBase((*NotImplementedError).HttpStatus(nil), format, args...),
	}
}

// IsNotImplemented checks if err is not implemented error.
func IsNotImplemented(err error) bool {
	return errors.Is(err, &NotImplementedError{})
}

func (e *NotImplementedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "operation not implemented"
}

func (e *NotImplementedError) HttpStatus() int {
	return http.StatusNotImplemented
}

type UnauthenticatedError struct {
	Base
}

func Unauthenticated(format string, args ...any) *UnauthenticatedError {
	return &UnauthenticatedError{
		Base: NewBase((*UnauthenticatedError).HttpStatus(nil), format, args...),
	}
}

func IsUnauthenticated(err error) bool {
	return errors.Is(err, &UnauthenticatedError{})
}

func (e *UnauthenticatedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "unauthenticated"
}

func (e *UnauthenticatedError) HttpStatus() int {
	return http.StatusUnauthorized
}

type UnauthorizedError struct {
	Base
}

func Unauthorized(format string, args ...any) *UnauthorizedError {
	return &UnauthorizedError{
		Base: NewBase((*UnauthorizedError).HttpStatus(nil), format, args...),
	}
}

func IsUnauthorized(err error) bool {
	return errors.Is(err, &UnauthorizedError{})
}

func (e *UnauthorizedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "unauthorized"
}

func (e *UnauthorizedError) HttpStatus() int {
	return http.StatusForbidden
}
