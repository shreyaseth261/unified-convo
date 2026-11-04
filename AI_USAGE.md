# AI usage

The service was implemented in Cursor with Grok 4.7. I used it for the HTTP routes, SQLite schema, webhook idempotency, status ordering, the outbound retry worker, SSE, and the tests. I reviewed the code paths and re-ran `go test -race ./...` after corrections. The notes below are the places the generated code was wrong.

## Latest SQLite required a Go toolchain this machine does not use

The first `go get modernc.org/sqlite` resolved v1.61.1 and rewrote `go.mod` to `go 1.26.0`. This machine has Go 1.25.6. The module line is what made that visible: v1.61.1 says `go 1.26.0`, and a later `go test` with `GOTOOLCHAIN=local` failed with `go.mod requires go >= 1.26.0`.

I checked the published `go.mod` files for older module versions. v1.59.0 declares `go 1.25.0` and depends on `modernc.org/libc v1.75.7` and `golang.org/x/sys v0.47.0`. `go mod tidy` had floated those indirect modules up to releases that themselves require Go 1.26, so pinning only the sqlite module was not enough. Pinning all three, setting `go 1.25.0`, and running `GOTOOLCHAIN=local go test -race ./...` is what actually proved the project builds with the installed toolchain.

## The test double did not implement the provider interface

The first test provider was generated with this method:

```go
func (s *scripted) Send(_ interface{ Done() <-chan struct{} }, msg provider.Outbound) (string, error)
```

That is not `provider.Sender`. `Send` takes `context.Context`. The same draft also declared a type and a function both named `messagePage`, which does not compile in one package. I replaced the method with the real signature and renamed the helper to `getMessages`. The race tests, including the early status callback that runs inside `Send`, pass after that change.
