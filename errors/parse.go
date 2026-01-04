package errors

import "strings"

import "strconv"

func parse(format string, args ...any) (string, any) {
	var (
		s      strings.Builder
		prev   rune
		param  string
		dollar bool
		lbrace bool
		pos    int
		n      int
		item   any
	)
	for _, ch := range format {
		if ch == '$' {
			dollar = true
			continue
		}
		if ch == '{' {
			lbrace = true
			continue
		}

		if ch == '}' {
			lbrace = false
			continue
		}

		// parse param
		if dollar && lbrace && ch != '}' && ch != ' ' && ch != 0 {
			param += string(ch)
			s.WriteString(string(ch))
			continue
		}

		if prev == '%' && ch == '%' {
			pos--
			prev = 0
		}

		if ch == '%' {
			pos++
			prev = '%'
		}

		if len(param) > 0 {
			item = param

			if param[0] == '%' {
				n = pos
				ndxString := ""
				ndxBegin := false
				for _, ch1 := range param {
					if ch1 == ']' {
						ndxBegin = false
						continue
					}
					if ch1 == '[' {
						ndxBegin = true
						continue
					}
					if ndxBegin {
						ndxString += string(ch1)
					}
				}
				if len(ndxString) > 0 {
					n, _ = strconv.Atoi(ndxString)
					if n > 0 && len(param) >= n {
						n--
					}
				}
				item = args[n]
			}
			dollar = false
			lbrace = false
		}

		s.WriteString(string(ch))
	}
	return s.String(), item
}
