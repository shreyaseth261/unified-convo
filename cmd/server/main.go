package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"unifiedconvo/internal/httpapi"
	"unifiedconvo/internal/inbox"
	"unifiedconvo/internal/provider"
	"unifiedconvo/internal/realtime"
	"unifiedconvo/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	port := getenv("PORT", "8080")
	dbPath := getenv("DB_PATH", "inbox.db")
	failRate, err := strconv.ParseFloat(getenv("SEND_FAIL_RATE", "0.2"), 64)
	if err != nil {
		return err
	}
	retryMS, err := strconv.Atoi(getenv("RETRY_BASE_MS", "200"))
	if err != nil {
		return err
	}

	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	hub := realtime.New()
	svc := inbox.New(st, provider.NewMock(failRate), hub, inbox.Config{
		RetryBase: time.Duration(retryMS) * time.Millisecond,
		Workers:   4,
	})
	if err := svc.Recover(context.Background()); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.New(svc, hub),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("inbox listening on %s (db %s)", srv.Addr, dbPath)
		errCh <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
