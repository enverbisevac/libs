package sqlt

import (
	"bytes"
	"fmt"
	"html/template"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// templateRegistry stores pre-parsed SQL templates
var templateRegistry = make(map[string]*template.Template)

// RegisterTemplate parses and stores a template in the registry
func RegisterTemplate(name, tmpl string) error {
	tmpl = strings.ReplaceAll(tmpl, "\n\t", " ")
	tmpl = strings.ReplaceAll(tmpl, "\n", " ")
	tmpl = strings.ReplaceAll(tmpl, "\t", " ")
	t, err := template.New(name).Funcs(template.FuncMap{
		"bind": func(value any) (string, error) {
			return "", nil
		},
		"LOWER": func(value string) string {
			return "LOWER(" + value + ")"
		},
		"mul":  func(a, b int) int { return a * b },
		"sub":  func(a, b int) int { return a - b },
		"add":  func(a, b int) int { return a + b },
		"div":  func(a, b int) int { return a / b },
		"mod":  func(a, b int) int { return a % b },
		"pow":  func(a, b int) int { return int(math.Pow(float64(a), float64(b))) },
		"sqrt": func(a int) int { return int(math.Sqrt(float64(a))) },
		"ceil": func(a float64) int { return int(math.Ceil(a)) },
	}).Parse(tmpl)
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", name, err)
	}
	templateRegistry[name] = t
	return nil
}

// ExecuteTemplate processes a registered SQL template and returns query + args
func ExecuteTemplate(name string, data any) (string, []any, error) {
	if err := detectSQLInjection(data); err != nil {
		return "", nil, err
	}

	var args []any
	var paramCount int
	var queryBuffer bytes.Buffer

	// Get the pre-parsed template
	t, ok := templateRegistry[name]
	if !ok {
		return "", nil, fmt.Errorf("template %s not found in registry", name)
	}

	t.Funcs(template.FuncMap{
		"bind": func(value any) (string, error) {
			_, ok := value.([]byte)
			if ok {
				paramCount++
				args = append(args, value)
				return fmt.Sprintf("$%d", paramCount), nil
			}

			arr, ok := convertToAnySlice(value)
			if ok {
				if len(arr) == 0 {
					return "(NULL)", nil // Avoid SQL syntax errors for empty slices
				}
				placeholders := make([]string, len(arr))
				for i := range arr {
					paramCount++
					placeholders[i] = "$" + strconv.Itoa(paramCount)
					args = append(args, arr[i])
				}
				return strings.Join(placeholders, ", "), nil
			}

			paramCount++
			args = append(args, value)
			return fmt.Sprintf("$%d", paramCount), nil
		},
	})

	if err := t.Execute(&queryBuffer, data); err != nil {
		return "", nil, err
	}

	return queryBuffer.String(), args, nil
}

// convertToAnySlice converts slices to []any
func convertToAnySlice(value any) ([]any, bool) {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice {
		return nil, false
	}

	slice := make([]any, v.Len())
	for i := 0; i < v.Len(); i++ {
		slice[i] = v.Index(i).Interface()
	}
	return slice, true
}

// detectSQLInjection checks for common SQL injection patterns using string operations.
func detectSQLInjection(input any) error {
	keywords := []string{
		"' or '1'='1", "' and '1'='1", "' --", "'/*", "' or 1=1", "' or 'a'='a",
	}

	checkString := func(str string) error {
		lowerStr := strings.ToLower(str)
		for _, keyword := range keywords {
			if strings.Contains(lowerStr, keyword) {
				return fmt.Errorf("potential SQL injection detected in %q keyword: %s", str, keyword)
			}
		}
		return nil
	}

	v := reflect.ValueOf(input)
	switch v.Kind() {
	case reflect.String:
		return checkString(v.String())
	case reflect.Map:
		for _, key := range v.MapKeys() {
			value := v.MapIndex(key)
			if value.Kind() == reflect.String {
				if err := checkString(value.String()); err != nil {
					return err
				}
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if field.Kind() == reflect.String {
				if err := checkString(field.String()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
