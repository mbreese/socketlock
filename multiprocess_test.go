package socketlock

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	helperOnce sync.Once
	helperPath string
	helperErr  error
)

func buildHelper(t *testing.T) string {
	t.Helper()
	helperOnce.Do(func() {
		dir, err := os.MkdirTemp("", "socketlock-helper-")
		if err != nil {
			helperErr = err
			return
		}
		helperPath = filepath.Join(dir, "socketlock-test")
		cmd := exec.Command("go", "build", "-o", helperPath, "./cmd/socketlock-test")
		cmd.Env = os.Environ()
		out, err := cmd.CombinedOutput()
		if err != nil {
			helperPath = ""
			helperErr = fmt.Errorf("go build helper: %v\n%s", err, string(out))
			return
		}
	})
	if helperErr != nil {
		t.Fatalf("build helper: %v", helperErr)
	}
	return helperPath
}

type procHandle struct {
	cmd       *exec.Cmd
	acquired  chan time.Time
	outputBuf *bytes.Buffer
	mu        *sync.Mutex
	doneCh    chan struct{}
	doneErr   error
	doneMu    sync.Mutex
}

func startHelper(t *testing.T, bin, path, mode string, hold time.Duration) *procHandle {
	t.Helper()
	args := []string{
		"-path", path,
		"-mode", mode,
		"-hold", hold.String(),
		"-policy", "writer",
		"-request-timeout", "5s",
		"-confirm-timeout", "5s",
		"-status-interval", "50ms",
	}
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	acquired := make(chan time.Time, 1)
	var buf bytes.Buffer
	var bufMu sync.Mutex
	scan := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			bufMu.Lock()
			buf.WriteString(line)
			buf.WriteString("\n")
			bufMu.Unlock()
			if strings.Contains(line, "acquired") {
				select {
				case acquired <- time.Now():
				default:
				}
			}
		}
	}
	go scan(stdout)
	go scan(stderr)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	handle := &procHandle{
		cmd:       cmd,
		acquired:  acquired,
		outputBuf: &buf,
		mu:        &bufMu,
		doneCh:    make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		handle.doneMu.Lock()
		handle.doneErr = err
		handle.doneMu.Unlock()
		close(handle.doneCh)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-handle.doneCh:
		default:
		}
	})
	return handle
}

func waitDone(p *procHandle, timeout time.Duration) (bool, error) {
	select {
	case <-p.doneCh:
		p.doneMu.Lock()
		err := p.doneErr
		p.doneMu.Unlock()
		return true, err
	case <-time.After(timeout):
		return false, nil
	}
}

func waitAcquired(t *testing.T, p *procHandle, timeout time.Duration) time.Time {
	t.Helper()
	select {
	case ts := <-p.acquired:
		return ts
	case <-p.doneCh:
		p.doneMu.Lock()
		err := p.doneErr
		p.doneMu.Unlock()
		p.mu.Lock()
		out := p.outputBuf.String()
		p.mu.Unlock()
		if strings.Contains(out, "acquired") {
			return time.Now()
		}
		if err != nil {
			t.Fatalf("process exit before acquire: %v\noutput:\n%s", err, out)
		}
		t.Fatalf("process exited before acquire; output:\n%s", out)
	case <-time.After(timeout):
		p.mu.Lock()
		out := p.outputBuf.String()
		p.mu.Unlock()
		t.Fatalf("timed out waiting for acquire; output:\n%s", out)
	}
	return time.Time{}
}

func waitExit(t *testing.T, p *procHandle, timeout time.Duration) {
	t.Helper()
	done, err := waitDone(p, timeout)
	if !done {
		p.mu.Lock()
		out := p.outputBuf.String()
		p.mu.Unlock()
		t.Fatalf("timed out waiting for process exit; output:\n%s", out)
	}
	if err != nil {
		p.mu.Lock()
		out := p.outputBuf.String()
		p.mu.Unlock()
		t.Fatalf("process exit: %v\noutput:\n%s", err, out)
	}
}

func TestMultiProcessWriteBlocksOnRead(t *testing.T) {
	path := newSocketPath(t)
	bin := buildHelper(t)

	reader := startHelper(t, bin, path, "read", 500*time.Millisecond)
	waitAcquired(t, reader, 2*time.Second)

	writer := startHelper(t, bin, path, "write", 100*time.Millisecond)
	writerStart := time.Now()

	select {
	case <-writer.acquired:
		t.Fatalf("writer acquired while read lock held")
	case <-writer.doneCh:
		writer.mu.Lock()
		out := writer.outputBuf.String()
		writer.mu.Unlock()
		t.Fatalf("writer exited early; output:\n%s", out)
	case <-time.After(150 * time.Millisecond):
	}

	waitExit(t, reader, 2*time.Second)
	acquiredAt := waitAcquired(t, writer, 2*time.Second)
	if acquiredAt.Sub(writerStart) < 350*time.Millisecond {
		t.Fatalf("writer acquired too soon after start: %s", acquiredAt.Sub(writerStart))
	}
	waitExit(t, writer, 2*time.Second)
}

func TestMultiProcessReadShare(t *testing.T) {
	path := newSocketPath(t)
	bin := buildHelper(t)

	readerA := startHelper(t, bin, path, "read", 300*time.Millisecond)
	waitAcquired(t, readerA, 2*time.Second)

	readerB := startHelper(t, bin, path, "read", 300*time.Millisecond)
	waitAcquired(t, readerB, 2*time.Second)

	waitExit(t, readerA, 2*time.Second)
	waitExit(t, readerB, 2*time.Second)
}

func TestMultiProcessWriterExclusive(t *testing.T) {
	path := newSocketPath(t)
	bin := buildHelper(t)

	writerA := startHelper(t, bin, path, "write", 300*time.Millisecond)
	waitAcquired(t, writerA, 2*time.Second)

	writerB := startHelper(t, bin, path, "write", 100*time.Millisecond)
	writerBStart := time.Now()

	select {
	case <-writerB.acquired:
		t.Fatalf("writer acquired while another writer held lock")
	case <-writerB.doneCh:
		writerB.mu.Lock()
		out := writerB.outputBuf.String()
		writerB.mu.Unlock()
		t.Fatalf("writer exited early; output:\n%s", out)
	case <-time.After(150 * time.Millisecond):
	}

	waitExit(t, writerA, 2*time.Second)
	acquiredAt := waitAcquired(t, writerB, 2*time.Second)
	if acquiredAt.Sub(writerBStart) < 250*time.Millisecond {
		t.Fatalf("writer acquired too soon after start: %s", acquiredAt.Sub(writerBStart))
	}
	waitExit(t, writerB, 2*time.Second)
}
