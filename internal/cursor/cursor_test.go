package cursor

import "testing"

func TestRoundTrip(t *testing.T) {
	inTS := "2026-10-28T10:15:00.000000000Z"
	inID := "abc123"
	gotTS, gotID, err := Decode(Encode(inTS, inID))
	if err != nil {
		t.Fatal(err)
	}
	if gotTS != inTS || gotID != inID {
		t.Fatalf("got (%s, %s)", gotTS, gotID)
	}
}

func TestEmpty(t *testing.T) {
	ts, id, err := Decode("")
	if err != nil || ts != "" || id != "" {
		t.Fatalf("ts=%q id=%q err=%v", ts, id, err)
	}
}

func TestRejectsGarbage(t *testing.T) {
	if _, _, err := Decode("not-a-cursor"); err == nil {
		t.Fatal("expected error")
	}
}
