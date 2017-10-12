package pulp

import (
	"testing"
)

func TestMkDir(t *testing.T) {
	fs := newFS()
	if fs.Find("/x/y") != nil {
		t.Errorf("Expected nil")
	}

	fs.Mkdir("/x/y/z")
	if fs.Find("/x") == nil ||
		fs.Find("/x/") == nil ||
		fs.Find("/x/y") == nil ||
		fs.Find("/x/y/") == nil ||
		fs.Find("/x/y/z") == nil ||
		fs.Find("/x/y/z/") == nil {
		t.Errorf("Unexpected nil")
	}

}
