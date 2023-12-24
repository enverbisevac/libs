package errors

import (
	"errors"
	"net/http"
	"testing"
)

func TestHttpStatus(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "conflict status",
			args: args{
				err: Conflict("article 123 already exist"),
			},
			want: http.StatusConflict,
		},
		{
			name: "not found status",
			args: args{
				err: NotFound("article 123 not found"),
			},
			want: http.StatusNotFound,
		},
		{
			name: "internal server error status",
			args: args{
				err: Internal(errors.New("fatal error"), "merge failed"),
			},
			want: http.StatusInternalServerError,
		},
		{
			name: "precondition failed status",
			args: args{
				err: PreconditionFailed("unable to commit"),
			},
			want: http.StatusPreconditionFailed,
		},
		{
			name: "validation status",
			args: args{
				err: Validation("name is mandatory field"),
			},
			want: http.StatusBadRequest,
		},
		{
			name: "not implemented status",
			args: args{
				err: NotImplemented("operation not implemented"),
			},
			want: http.StatusNotImplemented,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HttpStatus(tt.args.err); got != tt.want {
				t.Errorf("HttpStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
