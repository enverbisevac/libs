package errors

import (
	"testing"
)

func TestConflictWithDetail(t *testing.T) {
	type mergeConflict struct {
		Files []string `json:"files"`
	}

	base := "main"
	head := "dev"

	err := Conflict("merge failed for base '%s' and head '%s'", base, head).Details(mergeConflict{
		Files: []string{"test.txt"},
	})

	mc := Detail[mergeConflict](err)

	if mc.Files[0] != "test.txt" {
		t.Errorf("expected test.txt, got: %s", mc.Files[0])
	}
}
