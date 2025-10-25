// Copyright (c) 2023 Enver Bisevac
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package validator

import (
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/exp/constraints"
)

var RgxEmail = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

func NotBlank(value string) bool {
	return strings.TrimSpace(value) != ""
}

func MinRunes(value string, n int) bool {
	return utf8.RuneCountInString(value) >= n
}

func MaxRunes(value string, n int) bool {
	return utf8.RuneCountInString(value) <= n
}

func Between[T constraints.Ordered](value, min, max T) bool {
	return value >= min && value <= max
}

func Matches(value string, rx *regexp.Regexp) bool {
	return rx.MatchString(value)
}

func In[T comparable](value T, safelist ...T) bool {
	return slices.Contains(safelist, value)
}

func AllIn[T comparable](values []T, safelist ...T) bool {
	for i := range values {
		if !In(values[i], safelist...) {
			return false
		}
	}
	return true
}

func NotIn[T comparable](value T, blocklist ...T) bool {
	return !slices.Contains(blocklist, value)
}

func NoDuplicates[T comparable](values []T) bool {
	uniqueValues := make(map[T]bool)

	for _, value := range values {
		uniqueValues[value] = true
	}

	return len(values) == len(uniqueValues)
}

func IsEmail[T ~string](value T) bool {
	if len(value) > 254 {
		return false
	}

	return RgxEmail.MatchString(string(value))
}

func IsURL(value string) bool {
	u, err := url.ParseRequestURI(value)
	if err != nil {
		return false
	}

	return u.Scheme != "" && u.Host != ""
}
