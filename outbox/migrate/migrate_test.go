package migrate

import "testing"

func TestIsPostgres(t *testing.T) {
	tests := []struct {
		driver string
		want   bool
	}{
		{"postgres", true},
		{"pgx", true},
		{"sqlite3", false},
		{"sqlite", false},
		{"", false},
		{"PGX", false}, // case-sensitive on purpose: matches database/sql driver names verbatim
	}
	for _, tt := range tests {
		if got := isPostgres(tt.driver); got != tt.want {
			t.Errorf("isPostgres(%q) = %v, want %v", tt.driver, got, tt.want)
		}
	}
}