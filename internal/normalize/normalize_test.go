package normalize

import "testing"

func TestPhone(t *testing.T) {
	if got := Phone("+91 98765 43210"); got != "+919876543210" {
		t.Fatalf("got %s", got)
	}
	if got := Phone("+91-98765-43210"); got != "+919876543210" {
		t.Fatalf("got %s", got)
	}
	if !ValidPhone(Phone("+91 98765 43210")) {
		t.Fatal("expected valid")
	}
	if ValidPhone("123") {
		t.Fatal("short phone should be invalid")
	}
}

func TestEmail(t *testing.T) {
	if got := Email("  Agent@Example.com "); got != "agent@example.com" {
		t.Fatalf("got %s", got)
	}
	if !ValidEmail(Email("Agent@Example.com")) {
		t.Fatal("expected valid")
	}
	if ValidEmail("not-an-email") {
		t.Fatal("expected invalid")
	}
}
