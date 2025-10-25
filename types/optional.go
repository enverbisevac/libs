package types

import (
	"encoding/json"

	"github.com/swaggest/jsonschema-go"
)

type Optional[T any] struct {
	set   bool
	null  bool
	value T
}

func New[T any](value T) Optional[T] {
	return Optional[T]{
		value: value,
		set:   true,
		null:  false,
	}
}

func (n *Optional[T]) Set(value T) {
	n.value = value
	n.set = true
	n.null = false
}

func (n *Optional[T]) SetNull() {
	n.set = true
	n.null = true
}

func (n *Optional[T]) Unset() {
	n.set = false
	n.null = false
	var zero T
	n.value = zero
}

func (n Optional[T]) IsSet() bool { return n.set }

func (n Optional[T]) IsNull() bool { return n.set && n.null }

func (n Optional[T]) IsZero() bool {
	return !n.set
}

func (n Optional[T]) Value() (T, bool) {
	return n.value, n.set && !n.null
}

func (n Optional[T]) Ptr() *T {
	if n.set && !n.null {
		return &n.value
	}
	return nil
}

func (n Optional[T]) ValueOrDefault(defaultValue T) T {
	if n.set && !n.null {
		return n.value
	}
	return defaultValue
}

func (n Optional[T]) MustValue() T {
	if n.set && !n.null {
		return n.value
	}
	panic("nullable: value is not set")
}

// MarshalJSON handles JSON serialization
func (n Optional[T]) MarshalJSON() ([]byte, error) {
	if !n.set {
		return []byte("{}"), nil // Treat unset as {} in JSON
	}
	if n.null {
		return []byte("null"), nil
	}
	return json.Marshal(n.value)
}

// UnmarshalJSON handles JSON deserialization
func (n *Optional[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.SetNull()
		return nil
	}
	if string(data) == "{}" {
		n.Unset()
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	n.Set(v)
	return nil
}

func (n *Optional[T]) Enum() []any {
	var zero T
	c, ok := any(zero).(interface{ Enum() []any })
	if ok {
		return c.Enum()
	}
	return nil
}

func (n *Optional[T]) JSONSchema() (jsonschema.Schema, error) {
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
