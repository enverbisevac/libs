package errors

import (
	"reflect"
	"testing"
)

func Test_parse(t *testing.T) {
	type args struct {
		format string
		args   []any
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 any
	}{
		{
			name: "simple value",
			args: args{
				format: "article ${123} already exist",
			},
			want:  "article 123 already exist",
			want1: "123",
		},
		{
			name: "provide argument",
			args: args{
				format: "article ${%d} already exist",
				args:   []any{123},
			},
			want:  "article %d already exist",
			want1: any(123),
		},
		{
			name: "provide position argument",
			args: args{
				format: "article ${%[2]d} already exist",
				args:   []any{0, 123},
			},
			want:  "article %[2]d already exist",
			want1: any(123),
		},
		{
			name: "provide many arguments",
			args: args{
				format: "group %s and article ${%d} already exist",
				args:   []any{1, 123},
			},
			want:  "group %s and article %d already exist",
			want1: any(123),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parse(tt.args.format, tt.args.args...)
			if got != tt.want {
				t.Errorf("parse() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("parse() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
