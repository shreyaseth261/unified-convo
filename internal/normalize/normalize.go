// Package normalize canonicalizes phone numbers and email addresses so the
// same contact is recognized across webhook retries and channel formatting.
package normalize

import "strings"

// Phone strips formatting and keeps a leading plus when present.
// "+91 98765 43210" and "+91-98765-43210" both become "+919876543210".
func Phone(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '+' && b.Len() == 0:
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ValidPhone accepts E.164-sized numbers (8 to 15 digits), with an optional plus.
func ValidPhone(p string) bool {
	digits := p
	if strings.HasPrefix(p, "+") {
		digits = p[1:]
	}
	if len(digits) < 8 || len(digits) > 15 {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Email trims and lowercases an address.
func Email(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// ValidEmail is a deliberate lightweight check, not a full RFC parser.
func ValidEmail(s string) bool {
	if strings.ContainsAny(s, " \t") {
		return false
	}
	at := strings.LastIndex(s, "@")
	if at <= 0 || at >= len(s)-1 {
		return false
	}
	domain := s[at+1:]
	dot := strings.LastIndex(domain, ".")
	return dot > 0 && dot < len(domain)-1
}
