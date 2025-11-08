package optional

import (
	"encoding/json"

	"github.com/swaggest/jsonschema-go"
)

type Type[T any] struct {
	set   bool
	null  bool
	value T
}

func New[T any](value T) Type[T] {
	return Type[T]{
		value: value,
		set:   true,
		null:  false,
	}
}

func (t *Type[T]) Set(value T) {
	t.value = value
	t.set = true
	t.null = false
}

func (t *Type[T]) SetNull() {
	t.set = true
	t.null = true
}

func (t *Type[T]) Unset() {
	t.set = false
	t.null = false
	var zero T
	t.value = zero
}

func (t Type[T]) IsSet() bool { return t.set }

func (t Type[T]) IsNull() bool { return t.set && t.null }

func (t Type[T]) IsZero() bool {
	return !t.set
}

func (t Type[T]) Value() (T, bool) {
	return t.value, t.set && !t.null
}

func (t Type[T]) Ptr() *T {
	if t.set && !t.null {
		return &t.value
	}
	return nil
}

func (t Type[T]) ValueOrDefault(defaultValue T) T {
	if t.set && !t.null {
		return t.value
	}
	return defaultValue
}

func (t Type[T]) ValueOrZero() T {
	var zero T
	return t.ValueOrDefault(zero)
}

func (t Type[T]) MustValue() T {
	if t.set && !t.null {
		return t.value
	}
	panic("nullable: value is not set")
}

// MarshalJSON handles JSON serialization
func (t Type[T]) MarshalJSON() ([]byte, error) {
	if !t.set {
		return []byte("{}"), nil // Treat unset as {} in JSON
	}
	if t.null {
		return []byte("null"), nil
	}
	return json.Marshal(t.value)
}

// UnmarshalJSON handles JSON deserialization
func (t *Type[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		t.SetNull()
		return nil
	}
	if string(data) == "{}" {
		t.Unset()
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	t.Set(v)
	return nil
}

func (*Type[T]) Enum() []any {
	var zero T
	c, ok := any(zero).(interface{ Enum() []any })
	if ok {
		return c.Enum()
	}
	return nil
}

func (*Type[T]) JSONSchema() (jsonschema.Schema, error) {
	var schema jsonschema.Schema
	var zero T

	// Determine schema type based on zero value of T
	switch _typ := any(zero).(type) {
	case string:
		schema.WithType(jsonschema.String.Type())
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		schema.WithType(jsonschema.Integer.Type())
	case float32, float64:
		schema.WithType(jsonschema.Number.Type())
	case bool:
		schema.WithType(jsonschema.Boolean.Type())
	case []any:
		schema.WithType(jsonschema.Array.Type())
	case map[string]any:
		schema.WithType(jsonschema.Object.Type())
	default:
		obj, ok := _typ.(jsonschema.Exposer)
		if ok {
			schema, err := obj.JSONSchema()
			if err != nil {
				return schema, err
			}
		}
	}

	schema.AddType(jsonschema.Null)

	return schema, nil
}
