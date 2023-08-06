package timeutil

import "time"

type ParserFunc func(value string) (time.Time, error)

var (
	DefaultLayout     = time.RFC3339
	DefaultParserFunc = func(value string) (time.Time, error) {
		return time.Parse(DefaultLayout, value)
	}
)
