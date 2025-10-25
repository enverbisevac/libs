package types_test

import (
	"encoding/json"
	"testing"

	"github.com/enverbisevac/libs/types"
	"github.com/stretchr/testify/assert"
)

func TestNullable_SetAndGet(t *testing.T) {
	var n types.Optional[int]
	assert.False(t, n.IsSet())
	assert.False(t, n.IsNull())

	n.Set(42)
	val, ok := n.Value()
	assert.True(t, n.IsSet())
	assert.False(t, n.IsNull())
	assert.True(t, ok)
	assert.Equal(t, 42, val)
}

func TestNullable_SetNull(t *testing.T) {
	var n types.Optional[string]
	n.SetNull()

	assert.True(t, n.IsSet())
	assert.True(t, n.IsNull())

	val, ok := n.Value()
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func TestNullable_Unset(t *testing.T) {
	var n types.Optional[float64]
	n.Set(3.14)
	n.Unset()

	assert.False(t, n.IsSet())
	assert.False(t, n.IsNull())

	val, ok := n.Value()
	assert.False(t, ok)
	assert.Equal(t, 0.0, val)
}

func TestNullable_MarshalJSON_Value(t *testing.T) {
	var n types.Optional[int]
	n.Set(100)

	data, err := json.Marshal(n)
	assert.NoError(t, err)
	assert.JSONEq(t, "100", string(data))
}

func TestNullable_MarshalJSON_Null(t *testing.T) {
	var n types.Optional[string]
	n.SetNull()

	data, err := json.Marshal(n)
	assert.NoError(t, err)
	assert.JSONEq(t, "null", string(data))
}

func TestNullable_UnmarshalJSON_Value(t *testing.T) {
	var n types.Optional[int]
	err := json.Unmarshal([]byte("123"), &n)
	assert.NoError(t, err)

	val, ok := n.Value()
	assert.True(t, ok)
	assert.Equal(t, 123, val)
}

func TestNullable_UnmarshalJSON_Null(t *testing.T) {
	var n types.Optional[string]
	err := json.Unmarshal([]byte("null"), &n)
	assert.NoError(t, err)

	assert.True(t, n.IsSet())
	assert.True(t, n.IsNull())
}
