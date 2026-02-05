package socketlock

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "socketlock-")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "socketlock.sock")
}

func TestReadWriteExclusion(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	primary, err := Connect(ctx, path, LockConfig{
		Policy:         WriterPreferred,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         0,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer primary.Close()

	client2, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect client2: %v", err)
	}
	defer client2.Close()

	readLock, err := primary.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer writeCancel()
	if _, err := client2.AcquireWrite(writeCtx); err == nil {
		t.Fatalf("expected writer to block while reader active")
	}

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	writeCtx2, writeCancel2 := context.WithTimeout(context.Background(), time.Second)
	defer writeCancel2()
	writeLock, err := client2.AcquireWrite(writeCtx2)
	if err != nil {
		t.Fatalf("acquire write: %v", err)
	}
	if err := writeLock.Release(); err != nil {
		t.Fatalf("release write: %v", err)
	}
}

func TestWriterPreferredBlocksNewReaders(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	primary, err := Connect(ctx, path, LockConfig{
		Policy:         WriterPreferred,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         0,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer primary.Close()

	readerClient, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect reader client: %v", err)
	}
	defer readerClient.Close()

	writerClient, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect writer client: %v", err)
	}
	defer writerClient.Close()

	readLock, err := primary.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	writerReady := make(chan *Lock, 1)
	readerReady := make(chan *Lock, 1)

	go func() {
		lock, err := writerClient.AcquireWrite(ctx)
		if err == nil {
			writerReady <- lock
			return
		}
		writerReady <- nil
	}()

	time.Sleep(50 * time.Millisecond)

	go func() {
		lock, err := readerClient.AcquireRead(ctx)
		if err == nil {
			readerReady <- lock
			return
		}
		readerReady <- nil
	}()

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	select {
	case lock := <-readerReady:
		if lock != nil {
			lock.Release()
			t.Fatalf("reader acquired before writer under writer-preferred policy")
		}
	case lock := <-writerReady:
		if lock == nil {
			t.Fatalf("writer failed to acquire after readers released")
		}
		if err := lock.Release(); err != nil {
			t.Fatalf("release writer: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for writer to acquire")
	}

	select {
	case lock := <-readerReady:
		if lock == nil {
			t.Fatalf("reader failed to acquire after writer released")
		}
		if err := lock.Release(); err != nil {
			t.Fatalf("release reader: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for reader to acquire")
	}
}

func TestFIFOOrdering(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	primary, err := Connect(ctx, path, LockConfig{
		Policy:         FIFO,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         0,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer primary.Close()

	writerClient, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect writer client: %v", err)
	}
	defer writerClient.Close()

	readerClient, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect reader client: %v", err)
	}
	defer readerClient.Close()

	readLock, err := primary.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	writerCh := make(chan *Lock, 1)
	readerCh := make(chan *Lock, 1)

	go func() {
		lock, err := writerClient.AcquireWrite(ctx)
		if err == nil {
			writerCh <- lock
			return
		}
		writerCh <- nil
	}()

	go func() {
		lock, err := readerClient.AcquireRead(ctx)
		if err == nil {
			readerCh <- lock
			return
		}
		readerCh <- nil
	}()

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	writerLock := <-writerCh
	if writerLock == nil {
		t.Fatalf("writer failed to acquire")
	}

	select {
	case lock := <-readerCh:
		if lock != nil {
			lock.Release()
			t.Fatalf("reader acquired before writer released in FIFO")
		}
	case <-time.After(100 * time.Millisecond):
	}

	if err := writerLock.Release(); err != nil {
		t.Fatalf("release writer: %v", err)
	}

	readerLock := <-readerCh
	if readerLock == nil {
		t.Fatalf("reader failed to acquire after writer released")
	}
	if err := readerLock.Release(); err != nil {
		t.Fatalf("release reader: %v", err)
	}
}

func TestContextCancelAbortsPending(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	primary, err := Connect(ctx, path, LockConfig{
		Policy:         WriterPreferred,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         0,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer primary.Close()

	writerClient, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect writer client: %v", err)
	}
	defer writerClient.Close()

	readLock, err := primary.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	cancelCtx, cancelFn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelFn()
	if _, err := writerClient.AcquireWrite(cancelCtx); err == nil {
		t.Fatalf("expected writer to cancel while reader active")
	}

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	writeLock, err := writerClient.AcquireWrite(ctx)
	if err != nil {
		t.Fatalf("acquire write: %v", err)
	}
	if err := writeLock.Release(); err != nil {
		t.Fatalf("release write: %v", err)
	}
}

func TestSingleFlightEnforced(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	primary, err := Connect(ctx, path, LockConfig{
		Policy:         WriterPreferred,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         0,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer primary.Close()

	blocker, err := Connect(ctx, path, LockConfig{})
	if err != nil {
		t.Fatalf("connect blocker: %v", err)
	}
	defer blocker.Close()

	readLock, err := primary.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		lock, err := blocker.AcquireWrite(ctx)
		if err != nil {
			errCh <- err
			return
		}
		_ = lock.Release()
		errCh <- nil
	}()

	time.Sleep(20 * time.Millisecond)

	if _, err := blocker.AcquireRead(ctx); err == nil {
		t.Fatalf("expected multiple lock requests error")
	}

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("expected writer to acquire after reader released: %v", err)
	}
}

func TestMaxTTLRejectsRequest(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	client, err := Connect(ctx, path, LockConfig{
		Policy:         FIFO,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         time.Second,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	_, err = client.AcquireRead(WithTTL(ctx, 2*time.Second))
	if err == nil {
		t.Fatalf("expected ttl too long error")
	}
	if !strings.Contains(err.Error(), "timeout too long") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStatusLockTimeout(t *testing.T) {
	path := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	client, err := Connect(ctx, path, LockConfig{
		Policy:         FIFO,
		RequestTimeout: time.Second,
		ConfirmTimeout: time.Second,
		MaxTTL:         2 * time.Second,
		StatusInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	lock, err := client.AcquireRead(WithTTL(ctx, time.Second))
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}
	defer lock.Release()

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if lock.expired.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected lock to expire via status checks")
}
