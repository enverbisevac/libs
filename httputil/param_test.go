package httputil

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/enverbisevac/libs/validator"
)

func TestParamByNameOrDefault(t *testing.T) {
	r, err := http.NewRequest("GET", "/some-url?q=test&id=1&all=true&sort=1&sort=2&parent=", nil)
	if err != nil {
		t.Errorf("ParamByName() error = %v", err)
		return
	}
	t.Run("test string type", func(t *testing.T) {
		got, err := QueryParamOrDefault(r, "q", "")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != "test" {
			t.Errorf("ParamByName() = %v, want %v", got, "test")
		}
	})

	t.Run("test string type with empty value", func(t *testing.T) {
		got, err := QueryParamOrDefault(r, "parent", "default")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != "default" {
			t.Errorf("ParamByName() = %v, want %v", got, "default")
		}
	})

	t.Run("test int64 type", func(t *testing.T) {
		got, err := QueryParamOrDefault(r, "id", int64(0))
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != int64(1) {
			t.Errorf("ParamByName() = %v, want %v", got, 1)
		}
	})

	t.Run("test int64 type with validation", func(t *testing.T) {
		got, err := QueryParamOrDefault(r, "id", int64(5), func(v int64) error {
			if !validator.Between(v, 3, 7) {
				return fmt.Errorf("Value %v must be between 3 and 7", v)
			}
			return nil
		})
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != int64(5) {
			t.Errorf("ParamByName() = %v, want %v", got, 5)
		}
	})

	t.Run("test slice of int64", func(t *testing.T) {
		got, err := QueryParamOrDefault(r, "sort", []int64{})
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Errorf("ParamByName() = %v, want %v", got, []int64{1, 2})
		}
	})
}
