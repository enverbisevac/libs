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

func DecodeRequest(r *http.Request, decoder Decoder, body any, args ...any) error {
	err := decoder.Decode(&body)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	decode := httputil.NewDecoder(httputil.PathValue(chi.URLParam)).Decode

	for i := range args {
		err = decode(r, args[i])
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
