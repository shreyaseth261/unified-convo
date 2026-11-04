package store

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDSNRelativePathIsAbsoluteFileURI(t *testing.T) {
	got, err := dsn("inbox.db")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs("inbox.db")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "file://"+filepath.ToSlash(abs)) && !strings.HasPrefix(got, "file:///"+strings.TrimPrefix(filepath.ToSlash(abs), "/")) {
		t.Fatalf("dsn = %s", got)
	}
	if strings.Contains(got, "://inbox.db") {
		t.Fatalf("relative path was used as the URI host: %s", got)
	}
}
