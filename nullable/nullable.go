package nullable

import (
	"encoding/json"

	"github.com/swaggest/jsonschema-go"
)

type Nullable[T any] struct {
	set   bool
	null  bool
	value T
}

func New[T any](value T) Nullable[T] {
	return Nullable[T]{
		value: value,
		set:   true,
		null:  false,
	}
}
func (n *Nullable[T]) Set(value T) {
	n.value = value
	n.set = true
	n.null = false
}

func (n *Nullable[T]) SetNull() {
	n.set = true
	n.null = true
	var zero T
	n.value = zero
}

func (n *Nullable[T]) Unset() {
	n.set = false
	n.null = false
	var zero T
	n.value = zero
}

func (n Nullable[T]) IsSet() bool  { return n.set }
func (n Nullable[T]) IsNull() bool { return n.set && n.null }
func (n Nullable[T]) Value() (T, bool) {
	return n.value, n.set && !n.null
}

func (n Nullable[T]) ValueOrDefault(defaultValue T) T {
	if n.set && !n.null {
		return n.value
	}
	return defaultValue
}

func (n Nullable[T]) MustValue() T {
	if n.set && !n.null {
		return n.value
	}
	panic("nullable: value is not set")
}

// MarshalJSON handles JSON serialization
func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if !n.set {
		return []byte("null"), nil // Treat unset as null in JSON
	}
	if n.null {
		return []byte("null"), nil
	}
	return json.Marshal(n.value)
}

// UnmarshalJSON handles JSON deserialization
func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.SetNull()
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	n.Set(v)
	return nil
}

func (n *Nullable[T]) JSONSchema() (jsonschema.Schema, error) {
	var schema jsonschema.Schema
	var zero T

	// Determine schema type based on zero value of T
	switch any(zero).(type) {
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
		schema.WithType(jsonschema.Object.Type())
	}

	return schema, nil
}
