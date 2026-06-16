package openapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/enverbisevac/libs/openapi"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type GetParams struct {
	ID string `path:"id"`
}

type CreateBody struct {
	Name string `json:"name"`
}

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestOp_GetWithResponseBody(t *testing.T) {
	handler := openapi.Op(
		func(ctx openapi.Context, req openapi.Req[GetParams, openapi.None]) (*openapi.Resp[openapi.OK, User], error) {
			return &openapi.Resp[openapi.OK, User]{
				Body: User{ID: req.Header.ID, Name: "Alice"},
			}, nil
		},
		openapi.Meta{ID: "getUser"},
	)

	r := chi.NewRouter()
	r.Get("/users/{id}", handler.ServeHTTP)

	req := httptest.NewRequest(http.MethodGet, "/users/123", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var user User
	err := json.NewDecoder(w.Body).Decode(&user)
	require.NoError(t, err)
	assert.Equal(t, "123", user.ID)
	assert.Equal(t, "Alice", user.Name)
}

func TestOp_PostWithRequestAndResponseBody(t *testing.T) {
	handler := openapi.Op(
		func(ctx openapi.Context, req openapi.Req[openapi.None, CreateBody]) (*openapi.Resp[openapi.Created, User], error) {
			return &openapi.Resp[openapi.Created, User]{
				Body: User{ID: "new-id", Name: req.Body.Name},
			}, nil
		},
		openapi.Meta{ID: "createUser"},
	)

	body, _ := json.Marshal(CreateBody{Name: "Bob"})
	req := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var user User
	err := json.NewDecoder(w.Body).Decode(&user)
	require.NoError(t, err)
	assert.Equal(t, "new-id", user.ID)
	assert.Equal(t, "Bob", user.Name)
}

func TestOp_DeleteNoContent(t *testing.T) {
	handler := openapi.Op(
		func(ctx openapi.Context, req openapi.Req[GetParams, openapi.None]) (*openapi.Resp[openapi.NoContent, openapi.None], error) {
			return &openapi.Resp[openapi.NoContent, openapi.None]{}, nil
		},
		openapi.Meta{ID: "deleteUser"},
	)

	r := chi.NewRouter()
	r.Delete("/users/{id}", handler.ServeHTTP)

	req := httptest.NewRequest(http.MethodDelete, "/users/123", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestOp_ResponseHeaders(t *testing.T) {
	handler := openapi.Op(
		func(ctx openapi.Context, req openapi.Req[openapi.None, openapi.None]) (*openapi.Resp[openapi.OK, User], error) {
			return &openapi.Resp[openapi.OK, User]{
				Header: http.Header{"X-Custom": {"test-value"}},
				Body:   User{ID: "1", Name: "Alice"},
			}, nil
		},
		openapi.Meta{ID: "withHeaders"},
	)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test-value", w.Header().Get("X-Custom"))
}

func TestOp_HandlerError(t *testing.T) {
	handler := openapi.Op(
		func(ctx openapi.Context, req openapi.Req[openapi.None, openapi.None]) (*openapi.Resp[openapi.OK, User], error) {
			return nil, assert.AnError
		},
		openapi.Meta{ID: "errorHandler"},
	)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Contains(t, w.Body.String(), "assert.AnError")
}
