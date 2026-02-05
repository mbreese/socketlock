package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mbreese/socketlock"
)

func main() {
	var (
		socketPath     string
		policyStr      string
		requestTimeout time.Duration
		confirmTimeout time.Duration
		maxTTL         time.Duration
	)

	flag.StringVar(&socketPath, "socket", "", "unix socket path")
	flag.StringVar(&policyStr, "policy", "fifo", "policy: fifo, reader, writer")
	flag.DurationVar(&requestTimeout, "request-timeout", 30*time.Second, "request timeout")
	flag.DurationVar(&confirmTimeout, "confirm-timeout", 30*time.Second, "confirm timeout")
	flag.DurationVar(&maxTTL, "max-ttl", 0, "max ttl for lock requests (0 = unlimited)")
	flag.Parse()

	if strings.TrimSpace(socketPath) == "" {
		log.Fatalf("socket path is required")
	}

	policy, err := parsePolicy(policyStr)
	if err != nil {
		log.Fatalf("invalid policy: %v", err)
	}

	cfg := socketlock.LockConfig{
		Policy:         policy,
		RequestTimeout: requestTimeout,
		ConfirmTimeout: confirmTimeout,
		MaxTTL:         maxTTL,
		OnEvent: func(ev socketlock.Event) {
			log.Printf("%s client=%s lock=%s mode=%s ttl=%s reason=%s",
				ev.Type, ev.ClientID, ev.LockID, ev.Mode, ev.TTL, ev.Reason)
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := socketlock.StartServer(ctx, socketPath, cfg)
	if err != nil {
		log.Fatalf("start server: %v", err)
	}
	defer srv.Stop()

	<-ctx.Done()
}

func parsePolicy(value string) (socketlock.Policy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "reader", "read", "readerpreferred":
		return socketlock.ReaderPreferred, nil
	case "writer", "write", "writerpreferred":
		return socketlock.WriterPreferred, nil
	case "fifo":
		return socketlock.FIFO, nil
	default:
		return socketlock.FIFO, errors.New("expected fifo, reader, or writer")
	}
}
