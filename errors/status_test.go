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

	err := Conflict("merge failed for base '%s' and head '%s'", base, head).Detail(mergeConflict{
		Files: []string{"test.txt"},
	})

	if !IsConflict(err) {
		t.Errorf("expected conflict error, got: %s", err.Status)
	}

	mc := Detail[mergeConflict](err)

	if mc == nil || len(mc.Files) == 0 {
		t.Errorf("expected length of files should be 1, got: %d", len(mc.Files))
	}

	if mc.Files[0] != "test.txt" {
		t.Errorf("expected test.txt, got: %s", mc.Files[0])
	}
}
