package status

import "testing"

func TestAdvance(t *testing.T) {
	cases := []struct {
		current, next, want string
		changed             bool
	}{
		{Queued, Sent, Sent, true},
		{Queued, Delivered, Delivered, true},
		{Queued, Read, Read, true},
		{Queued, Failed, Failed, true},
		{Sent, Delivered, Delivered, true},
		{Sent, Queued, Sent, false},
		{Sent, Failed, Failed, true},
		{Delivered, Sent, Delivered, false},
		{Delivered, Read, Read, true},
		{Delivered, Failed, Delivered, false},
		{Read, Delivered, Read, false},
		{Read, Failed, Read, false},
		{Failed, Sent, Failed, false},
		{Failed, Read, Failed, false},
		{Sent, Sent, Sent, false},
	}
	for _, tc := range cases {
		got, changed := Advance(tc.current, tc.next)
		if got != tc.want || changed != tc.changed {
			t.Errorf("Advance(%s, %s) = (%s, %v), want (%s, %v)", tc.current, tc.next, got, changed, tc.want, tc.changed)
		}
	}
}
