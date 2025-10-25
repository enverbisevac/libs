package errors

import (
	"net/http"
	"testing"
)

func TestJSONResponse(t *testing.T) {
	type args struct {
		w   http.ResponseWriter
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := JSONResponse(tt.args.w, tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("JSONResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
