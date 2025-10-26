package errors

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"strings"
	"time"
)

type Base struct {
	// Msg contains user friendly error.
	Msg        string    `json:"message"`
	StatusCode string    `json:"status,omitempty"`
	TraceID    string    `json:"trace_id,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

func newBase(msg string) Base {
	return Base{
		Msg:       msg,
		Timestamp: time.Now(),
	}
}

func newBasef(format string, args ...any) Base {
	return Base{
		Msg:       fmt.Sprintf(format, args...),
		Timestamp: time.Now(),
	}
}

func (b *Base) SetTraceID(id string) {
	b.TraceID = id
}

func (b *Base) IsEqual(other Base) bool {
	return b.Msg == other.Msg && b.StatusCode == other.StatusCode
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
	Item   any    `json:"item,omitempty"`
	Errors Errors `json:"errors"`
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
		Base: newBasef(format, args...),
	}
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

func (e *ConflictError) ErrorDetails() string {
	if len(e.Errors) == 0 {
		return e.Error()
	}
	sb := strings.Builder{}
	sb.WriteString(e.Error())
	sb.WriteString("\n\n")
	for _, err := range e.Errors {
		sb.WriteString("\t - " + err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

// HttpResponse returns http response for ConflictError.
func (e *ConflictError) HttpResponse() HttpResponse {
	slice := make([]string, len(e.Errors))
	for i, e := range e.Errors {
		slice[i] = e.Error()
	}
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusConflict,
		Errors: slice,
	}
}

func (e *ConflictError) AddError(err error) *ConflictError {
	if err != nil {
		e.Errors = append(e.Errors, err)
	}
	return e
}

// Is checks if err is ConflictError.
func (e *ConflictError) Is(err error) bool {
	ce, ok := err.(*ConflictError)
	if !ok {
		return false
	}
	return e.IsEqual(ce.Base) && reflect.DeepEqual(e.Item, ce.Item)
}

// AsConflict return err as ConflictError or nil if it is not
// successfull.
func AsConflict(err error) (cerr *ConflictError, b bool) {
	if errors.As(err, &cerr) {
		return cerr, true
	}

	return nil, false
}

// IsConflict checks if err is conflict error.
func IsConflict(err error) bool {
	_, ok := AsConflict(err)
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
		Base: newBasef(format, args...),
	}
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

func (e *NotFoundError) ErrorDetails() string {
	return e.Error()
}

// HttpResponse returns http response for NotFoundError.
func (e *NotFoundError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusNotFound,
	}
}

func (e *NotFoundError) Is(err error) bool {
	nfe, ok := err.(*NotFoundError)
	if !ok {
		return false
	}
	return e.IsEqual(nfe.Base) &&
		reflect.DeepEqual(e.Item, nfe.Item)
}

// AsNotFound return err as NotFoundError or nil if it is not
// successfull.
func AsNotFound(err error) (nerr *NotFoundError, b bool) {
	if errors.As(err, &nerr) {
		return nerr, true
	}

	return nil, false
}

// IsNotFound checks if err is not found error.
func IsNotFound(err error) bool {
	_, ok := AsNotFound(err)
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
		Base:       newBasef(format, args...),
		Err:        err,
		Stacktrace: debug.Stack(),
	}
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

func (e *InternalError) ErrorDetails() string {
	if e.Err == nil {
		return e.Error()
	}

	sb := strings.Builder{}
	sb.WriteString(e.Error())
	sb.WriteString("\n\n")

	if e.Err != nil {
		sb.WriteString("Caused by:\n")
		sb.WriteString("\t" + e.Err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

// HttpResponse returns http response for InternalError.
func (e *InternalError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusInternalServerError,
		Errors: []string{cmp.Or(e.Err, New("Internal Server Error")).Error()},
	}
}

func (e *InternalError) Is(err error) bool {
	ie, ok := err.(*InternalError)
	if !ok {
		return false
	}
	return e.IsEqual(ie.Base) &&
		errors.Is(e.Err, ie.Err)
}

// AsInternal return err as InternalError or nil if it is not
// successfull.
func AsInternal(err error) (ierr *InternalError, b bool) {
	if errors.As(err, &ierr) {
		return ierr, true
	}

	return nil, false
}

// IsInternal checks if err is internal error.
func IsInternal(err error) bool {
	_, ok := AsInternal(err)
	return ok
}

type PreconditionFailedError struct {
	Base
	Err error
}

// Internal is a helper function to return an internal Error.
func PreconditionFailed(format string, args ...any) *PreconditionFailedError {
	return &PreconditionFailedError{
		Base: newBasef(format, args...),
	}
}

func (e *PreconditionFailedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "precondition failed error"
}

func (e *PreconditionFailedError) ErrorDetails() string {
	if e.Err == nil {
		return e.Error()
	}
	sb := strings.Builder{}
	sb.WriteString(e.Error())
	sb.WriteString("\n\n")

	if e.Err != nil {
		sb.WriteString("Caused by:\n")
		sb.WriteString("\t" + e.Err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

func (e *PreconditionFailedError) Is(err error) bool {
	pe, ok := err.(*PreconditionFailedError)
	if !ok {
		return false
	}
	return e.IsEqual(pe.Base) &&
		errors.Is(e.Err, pe.Err)
}

// HttpResponse returns http response for PreconditionFailedError.
func (e *PreconditionFailedError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusPreconditionFailed,
		Errors: []string{e.Err.Error()},
	}
}

func (e *PreconditionFailedError) SetErr(err error) *PreconditionFailedError {
	e.Err = err
	return e
}

func AsPreconditionFailed(err error) (perr *PreconditionFailedError, b bool) {
	if errors.As(err, &perr) {
		return perr, true
	}

	return nil, false
}

// IsInternal checks if err is internal error.
func IsPreconditionFailed(err error) bool {
	_, ok := AsPreconditionFailed(err)
	return ok
}

type ValidationError struct {
	Base
	Errors Errors `json:"errors,omitempty"`
}

// Validation is a helper function to return an invalid argument Error.
func Validation(format string, args ...any) *ValidationError {
	return &ValidationError{
		Base: newBasef(format, args...),
	}
}

func (e *ValidationError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "validation error"
}

func (e *ValidationError) ErrorDetails() string {
	if len(e.Errors) == 0 {
		return e.Error()
	}
	sb := strings.Builder{}
	sb.WriteString(e.Error())
	sb.WriteString("\n\n")

	for _, err := range e.Errors {
		sb.WriteString("\t - " + err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

func (e *ValidationError) AsError() error {
	if e == nil || (len(e.Errors) == 0 && e.Msg == "") {
		return nil
	}
	return e
}

// HttpResponse returns http response for ValidationError.
func (e *ValidationError) HttpResponse() HttpResponse {
	slice := make([]string, len(e.Errors))
	for i, e := range e.Errors {
		slice[i] = e.Error()
	}
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusBadRequest,
		Errors: slice,
	}
}

func (e *ValidationError) AddError(err error) *ValidationError {
	if err != nil {
		e.Errors = append(e.Errors, err)
	}
	return e
}

func (e *ValidationError) Is(err error) bool {
	ve, ok := err.(*ValidationError)
	if !ok {
		return false
	}
	return e.IsEqual(ve.Base) &&
		reflect.DeepEqual(e.Errors, ve.Errors)
}

func (e *ValidationError) Check(ok bool, err error) {
	if !ok {
		_ = e.AddError(err)
	}
}

func AsValidation(err error) (verr *ValidationError, b bool) {
	if errors.As(err, &verr) {
		return verr, true
	}

	return nil, false
}

// IsValidation checks if err is invalid argument error.
func IsValidation(err error) bool {
	_, ok := AsValidation(err)
	return ok
}

type NotImplementedError struct {
	Base
}

// InvalidArgument is a helper function to return an invalid argument Error.
func NotImplemented(format string, args ...any) *NotImplementedError {
	return &NotImplementedError{
		Base: newBasef(format, args...),
	}
}

func (e *NotImplementedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "operation not implemented"
}

func (e *NotImplementedError) ErrorDetails() string {
	return e.Error()
}

// HttpResponse returns http response for NotImplementedError.
func (e *NotImplementedError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusNotImplemented,
	}
}

func (e *NotImplementedError) Is(err error) bool {
	nie, ok := err.(*NotImplementedError)
	if !ok {
		return false
	}
	return e.IsEqual(nie.Base)
}

func AsNotImplemented(err error) (nerr *NotImplementedError, b bool) {
	if errors.As(err, &nerr) {
		return nerr, true
	}

	return nil, false
}

// IsNotImplemented checks if err is not implemented error.
func IsNotImplemented(err error) bool {
	_, ok := AsNotImplemented(err)
	return ok
}

type UnauthenticatedError struct {
	Base
}

func Unauthenticated(format string, args ...any) *UnauthenticatedError {
	return &UnauthenticatedError{
		Base: newBasef(format, args...),
	}
}

func (e *UnauthenticatedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "unauthenticated"
}

func (e *UnauthenticatedError) ErrorDetails() string {
	return e.Error()
}

// HttpResponse returns http response for UnauthenticatedError.
func (e *UnauthenticatedError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusUnauthorized,
	}
}

func AsUnautenticated(err error) (verr *ValidationError, b bool) {
	if errors.As(err, &verr) {
		return verr, true
	}

	return nil, false
}

func IsUnauthenticated(err error) bool {
	_, ok := AsUnautenticated(err)
	return ok
}

type UnauthorizedError struct {
	Base
}

func Unauthorized(format string, args ...any) *UnauthorizedError {
	return &UnauthorizedError{
		Base: newBasef(format, args...),
	}
}

func (e *UnauthorizedError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "unauthorized"
}

// HttpResponse returns http response for UnauthorizedError.
func (e *UnauthorizedError) HttpResponse() HttpResponse {
	return HttpResponse{
		Base:   e.Base,
		Status: http.StatusForbidden,
	}
}

func (e *UnauthorizedError) ErrorDetails() string {
	return e.Error()
}

func AsUnauthorized(err error) (verr *ValidationError, b bool) {
	if errors.As(err, &verr) {
		return verr, true
	}

	return nil, false
}

func IsUnauthorized(err error) bool {
	_, ok := AsUnauthorized(err)
	return ok
}

func Details(err error) error {
	if err == nil {
		return nil
	}

	var ok bool

	var ve *ValidationError
	ok = errors.As(err, &ve)
	if ok {
		return errors.New(ve.ErrorDetails())
	}

	var ce *ConflictError
	ok = errors.As(err, &ce)
	if ok {
		return errors.New(ce.ErrorDetails())
	}

	var ne *NotFoundError
	ok = errors.As(err, &ne)
	if ok {
		return errors.New(ne.ErrorDetails())
	}

	var ie *InternalError
	ok = errors.As(err, &ie)
	if ok {
		return errors.New(ie.ErrorDetails())
	}

	var nie *NotImplementedError
	ok = errors.As(err, &nie)
	if ok {
		return errors.New(nie.ErrorDetails())
	}

	var pe *PreconditionFailedError
	ok = errors.As(err, &pe)
	if ok {
		return errors.New(pe.ErrorDetails())
	}

	var ue *UnauthenticatedError
	ok = errors.As(err, &ue)
	if ok {
		return errors.New(ue.ErrorDetails())
	}

	var aue *UnauthorizedError
	ok = errors.As(err, &aue)
	if ok {
		return errors.New(aue.ErrorDetails())
	}

	return err
}

type Errors []error

func (me Errors) MarshalJSON() ([]byte, error) {
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
