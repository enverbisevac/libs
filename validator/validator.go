package validator

import (
	"sync"
	"time"

	"github.com/enverbisevac/libs/errors"
)

type ValidatorFunc[T any] func(T) error

type Validator struct {
	mux    sync.Mutex
	Errors []error
}

func (v *Validator) HasErrors() bool {
	v.mux.Lock()
	defer v.mux.Unlock()
	return len(v.Errors) != 0
}

func (v *Validator) AddError(err ...error) {
	if err == nil {
		return
	}

	nerrs := make([]error, 0, len(err))
	for _, verr := range err {
		if verr != nil {
			nerrs = append(nerrs, verr)
		}
	}

	if v.Errors == nil {
		v.Errors = []error{}
	}

	v.mux.Lock()
	defer v.mux.Unlock()

	v.Errors = append(v.Errors, nerrs...)
}

func (v *Validator) Check(ok bool, err error) {
	if !ok {
		v.AddError(err)
	}
}

func (v *Validator) Err(msg string) error {
	if v.HasErrors() {
		return &errors.ValidationError{
			Base: errors.Base{
				Msg:       msg,
				Status:    (*errors.ValidationError).HttpStatus(nil),
				Timestamp: time.Now(),
			},
			Errors: v.Errors,
		}
	}
	return nil
}

func Validate[T any](data T, validators ...ValidatorFunc[T]) error {
	for _, validator := range validators {
		err := validator(data)
		if err != nil {
			return err
		}
	}
	return nil
}

func FromError(err error) (v *Validator) {
	v = new(Validator)
	verr, ok := errors.AsValidation(err)
	if ok {
		v.Errors = verr.Errors
	}
	return
}
