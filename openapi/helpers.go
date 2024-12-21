package openapi

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"reflect"
	"runtime"

	"github.com/enverbisevac/libs/errors"
	"github.com/enverbisevac/libs/httputil"
	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

func GetStatus[T Success](object *T) int {
	var status T
	statusCode := http.StatusOK
	stat, ok := any(status).(HttpStatus)
	if ok {
		statusCode = stat.HttpStatus()
	}
	return statusCode
}

func Handle(
	handler http.Handler,
	options ...OperationFunc,
) *Operation {
	cOp := Operation{
		ID:      nameOf(handler),
		Handler: handler,
	}

	for _, fn := range options {
		fn(&cOp)
	}

	for _, err := range cOp.Errors {
		resp, ok := err.(HttpResponse)
		if ok {
			cOp.Responses[resp.HttpResponse().Status] = err
		}
	}

	handlers := make([]httputil.Constructor, len(cOp.handlers))
	for i, fn := range cOp.handlers {
		handlers[i] = httputil.Constructor(fn)
	}

	cOp.Handler = httputil.NewChain(handlers...).Then(cOp.Handler)

	return &cOp
}

func EncodeResponse[T Success](w http.ResponseWriter, encoder Encoder, value any, status *T) {
	stat, ok := any(status).(HttpStatus)
	if ok {
		w.WriteHeader(stat.HttpStatus())
		if stat.HttpStatus() == http.StatusNoContent {
			return
		}
	}

	if value == nil {
		return
	}

	err := encoder.Encode(value)
	if err != nil {
		errors.Response(encoder, w, err)
	}
}

func DecodeRequest(r *http.Request, decoder Decoder, body any, args ...any) error {
	err := decoder.Decode(&body)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	for i := range args {
		err = httputil.Decode(r, chi.URLParam, args[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func GetEncDec(w http.ResponseWriter, r *http.Request) (Encoder, Decoder) {
	contentType := r.Header.Get("Content-Type")
	accept := r.Header.Get("Accept")

	var decoder Decoder

	switch contentType {
	default:
		fallthrough
	case "application/json":
		decoder = json.NewDecoder(r.Body)
	case "application/yaml":
		decoder = yaml.NewDecoder(r.Body)
	}

	var encoder Encoder

	switch accept {
	case "application/json":
		encoder = json.NewEncoder(w)
	case "application/xml":
		encoder = xml.NewEncoder(w)
	case "application/yaml":
		encoder = yaml.NewEncoder(w)
	default:
		encoder = json.NewEncoder(w)
	}

	return encoder, decoder
}

func nameOf(f any) string {
	v := reflect.ValueOf(f)
	if v.Kind() == reflect.Func {
		if rf := runtime.FuncForPC(v.Pointer()); rf != nil {
			return rf.Name()
		}
	}
	return v.String()
}
