package httputil

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/enverbisevac/libs/timeutil"
	"github.com/enverbisevac/libs/validator"
)

func TestParamByNameOrDefault(t *testing.T) {
	timeutil.DefaultLayout = time.DateOnly
	r, err := http.NewRequest("GET", "/some-url?id=1&parent=", nil)
	if err != nil {
		t.Errorf("ParamByName() error = %v", err)
		return
	}

	t.Run("test string type with empty value", func(t *testing.T) {
		got := QueryParamOrDefault(r, "parent", "default")
		if got != "default" {
			t.Errorf("ParamByName() = %v, want %v", got, "default")
		}
	})

	t.Run("test int64 type with validation", func(t *testing.T) {
		got := QueryParamOrDefault(r, "id", int64(5), func(v int64) error {
			if !validator.Between(v, 3, 7) {
				return fmt.Errorf("Value %v must be between 3 and 7", v)
			}
			return nil
		})
		if got != int64(5) {
			t.Errorf("ParamByName() = %v, want %v", got, 5)
		}
	})
}

func TestParamByName(t *testing.T) {
	timeutil.DefaultLayout = time.DateOnly
	r, err := http.NewRequest("GET", "/some-url?q=test&id=1&all=true&sort=1&sort=2&parent=&time=2023-08-01", nil)
	if err != nil {
		t.Errorf("ParamByName() error = %v", err)
		return
	}
	t.Run("test string type", func(t *testing.T) {
		got, err := QueryParam[string](r, "q")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != "test" {
			t.Errorf("ParamByName() = %v, want %v", got, "test")
		}
	})

	t.Run("test int64 type", func(t *testing.T) {
		got, err := QueryParam[int64](r, "id")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if got != int64(1) {
			t.Errorf("ParamByName() = %v, want %v", got, 1)
		}
	})

	t.Run("test slice of int64", func(t *testing.T) {
		got, err := QueryParam[[]int64](r, "sort")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		if !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Errorf("ParamByName() = %v, want %v", got, []int64{1, 2})
		}
	})

	t.Run("test time.Time type", func(t *testing.T) {
		got, err := QueryParam[time.Time](r, "time")
		if err != nil {
			t.Errorf("ParamByName() error = %v", err)
			return
		}
		date := time.Date(2023, 8, 1, 0, 0, 0, 0, time.Local)
		if got.Year() != date.Year() && got.Month() != date.Month() && got.Day() != date.Day() {
			t.Errorf("ParamByName() = %v, want %v", got, date)
		}
	})
}
