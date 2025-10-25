package httputil

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

type RequestURLParam func(r *http.Request, key string) string

// Decode an HTTP request into the provided struct
func Decode(r *http.Request, fn RequestURLParam, data any) error {
	typ := reflect.TypeOf(data)
	if typ == nil {
		return fmt.Errorf("invalid decode type: nil")
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("invalid decode type: %v", typ.Kind())
	}

	return decodeRequest(r, typ, func(key string) string {
		return fn(r, key)
	}, data)
}

func decodeRequest(r *http.Request, t reflect.Type, fn URLParam, data any) error {
	_, err := decodeStruct(r, t, fn, data)
	if err != nil {
		return err
	}
	// if !body {
	// 	err := decodeBody(r, data)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func decodeStruct(r *http.Request, t reflect.Type, fn URLParam, data any) (bool, error) {
	query := r.URL.Query()
	body := false
	for i := 0; i < t.NumField(); i++ {
		typ := t.Field(i)
		field := reflect.ValueOf(data).Elem().Field(i)

		if typ.Type.Kind() == reflect.Struct {
			var err error
			if body, err = decodeStruct(r, typ.Type, fn, field.Addr().Interface()); err != nil {
				return body, err
			}
		}

		if queryTag := typ.Tag.Get("query"); queryTag != "" {
			if err := decodeQuery(field, typ.Type, query, queryTag); err != nil {
				return body, err
			}
		}

		if pathTag := typ.Tag.Get("path"); pathTag != "" {
			if err := decodePath(field, typ.Type, fn, pathTag); err != nil {
				return body, err
			}
		}

		if headerTag := typ.Tag.Get("header"); headerTag != "" {
			if err := decodeHeader(field, typ.Type, r.Header, headerTag); err != nil {
				return body, err
			}
		}

		// bodyTag := typ.Tag.Get("body")
		// if bodyTag != "" {
		// 	body = true
		// 	if err := decodeBody(r, field.Addr().Interface()); err != nil {
		// 		return body, err
		// 	}
		// }
	}
	return body, nil
}

func decodeQuery(field reflect.Value, typ reflect.Type, query url.Values, tag string) error {
	parts := strings.Split(tag, ",")
	if query.Has(parts[0]) {
		if field.Kind() == reflect.Slice {
			var explode bool
			for _, p := range parts[1:] {
				if p == "explode" {
					explode = true
				}
			}

			var value []string
			if explode {
				value = query[parts[0]]
			} else {
				value = strings.Split(query.Get(parts[0]), ",")
			}

			if err := resolveValues(field, typ, value); err != nil {
				return err
			}
			return nil
		}
		if err := resolveValue(field, typ, query.Get(parts[0])); err != nil {
			return err
		}
	}
	return nil
}

type URLParam func(key string) string

func decodePath(field reflect.Value, typ reflect.Type, fn URLParam, tag string) error {
	if path := fn(tag); path != "" {
		if err := resolveValue(field, typ, path); err != nil {
			return err
		}
	}
	return nil
}

func decodeHeader(field reflect.Value, typ reflect.Type, header http.Header, tag string) error {
	if field.Kind() == reflect.Slice {
		if err := resolveValues(field, typ, header.Values(tag)); err != nil {
			return err
		}
		return nil
	}
	if header.Get(tag) != "" {
		if err := resolveValue(field, typ, header.Get(tag)); err != nil {
			return err
		}
	}
	return nil
}
