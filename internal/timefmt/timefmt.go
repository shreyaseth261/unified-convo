package timefmt

import "time"

// Layout is a fixed-width UTC timestamp. Lexical order matches chronological order,
// which cursor comparisons rely on.
const Layout = "2006-01-02T15:04:05.000000000Z"

// Format renders t in Layout.
func Format(t time.Time) string {
	return t.UTC().Format(Layout)
}
