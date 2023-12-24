package errors

import (
	"encoding/json"
	"net/http"
)

func HttpStatus(err error) int {
	type status interface {
		HttpStatus() int
	}

	v, ok := err.(status)
	if ok {
		return v.HttpStatus()
	}
	return http.StatusInternalServerError
}

func JSONResponse(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-Type", "application/problem+json")
	status := HttpStatus(err)
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(err)
}
