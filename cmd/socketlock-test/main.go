package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mbreese/socketlock"
)

func main() {
	var (
		path      string
		mode      string
		hold      time.Duration
		policy    string
		reqTO     time.Duration
		confirmTO time.Duration
		maxTTL    time.Duration
		statusInt time.Duration
		ttlSecs   int
	)
	flag.StringVar(&path, "path", "", "unix socket path (required)")
	flag.StringVar(&mode, "mode", "read", "lock mode: read or write")
	flag.DurationVar(&hold, "hold", 5*time.Second, "how long to hold the lock")
	flag.StringVar(&policy, "policy", "writer", "policy for primary: reader, writer, fifo")
	flag.DurationVar(&reqTO, "request-timeout", 30*time.Second, "request timeout (time to receive offer)")
	flag.DurationVar(&confirmTO, "confirm-timeout", 30*time.Second, "confirm timeout (time to confirm offer)")
	flag.DurationVar(&maxTTL, "max-ttl", 0, "maximum allowed TTL (0 = unlimited)")
	flag.DurationVar(&statusInt, "status-interval", 10*time.Second, "status heartbeat interval")
	flag.IntVar(&ttlSecs, "ttl", 0, "requested TTL in seconds (0 = no TTL)")
	flag.Parse()

	if strings.TrimSpace(path) == "" {
		log.Fatal("-path is required")
	}

	cfg := socketlock.LockConfig{
		Policy:         parsePolicy(policy),
		RequestTimeout: reqTO,
		ConfirmTimeout: confirmTO,
		MaxTTL:         maxTTL,
		StatusInterval: statusInt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := socketlock.Connect(ctx, path, cfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	var lock *socketlock.Lock
	mode = strings.ToLower(mode)
	switch mode {
	case "read", "r":
		if ttlSecs > 0 {
			lock, err = client.AcquireRead(socketlock.WithTTL(ctx, time.Duration(ttlSecs)*time.Second))
		} else {
			lock, err = client.AcquireRead(ctx)
		}
	case "write", "w":
		if ttlSecs > 0 {
			lock, err = client.AcquireWrite(socketlock.WithTTL(ctx, time.Duration(ttlSecs)*time.Second))
		} else {
			lock, err = client.AcquireWrite(ctx)
		}
	default:
		log.Fatalf("unknown mode %q", mode)
	}
	if err != nil {
		log.Fatalf("acquire %s: %v", mode, err)
	}

	fmt.Printf("acquired %s lock; holding for %s\n", mode, hold)
	time.Sleep(hold)

	if err := lock.Release(); err != nil {
		log.Fatalf("release: %v", err)
	}
	fmt.Println("released")
}

func parsePolicy(value string) socketlock.Policy {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "reader", "read", "readers":
		return socketlock.ReaderPreferred
	case "writer", "write", "writers":
		return socketlock.WriterPreferred
	case "fifo":
		return socketlock.FIFO
	default:
		return socketlock.WriterPreferred
	}
}
