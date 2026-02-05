package socketlock

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	listener       net.Listener
	manager        *lockManager
	stopCh         chan struct{}
	wg             sync.WaitGroup
	requestTimeout time.Duration
	confirmTimeout time.Duration
	maxTTL         time.Duration
}

func newServer(listener net.Listener, cfg LockConfig) *server {
	return &server{
		listener:       listener,
		manager:        newLockManager(cfg.Policy),
		stopCh:         make(chan struct{}),
		requestTimeout: defaultTimeout(cfg.RequestTimeout),
		confirmTimeout: defaultTimeout(cfg.ConfirmTimeout),
		maxTTL:         cfg.MaxTTL,
	}
}

func (s *server) start() {
	s.wg.Add(1)
	go s.serve()
}

func (s *server) stop() error {
	select {
	case <-s.stopCh:
		return nil
	default:
		close(s.stopCh)
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *server) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
			}
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	reqs := make(map[string]*request)
	var reqsMu sync.Mutex

	closeCh := make(chan struct{})
	go func() {
		<-closeCh
		reqsMu.Lock()
		defer reqsMu.Unlock()
		for _, req := range reqs {
			s.manager.onConnClosed(req)
		}
	}()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = err
			}
			close(closeCh)
			return
		}
		clientID, cmd, arg, parseErr := parseCommand(line)
		if parseErr != nil {
			continue
		}

		switch cmd {
		case "HELLO":
			line := fmt.Sprintf("%s CONNECTED\n", clientID)
			_, _ = conn.Write([]byte(line))
		case "EOL":
			close(closeCh)
			return
		case "REQREAD", "REQWRITE":
			mode := modeRead
			if cmd == "REQWRITE" {
				mode = modeWrite
			}
			lockID, ttl, ttlErr := parseLockAndTTL(arg)
			if ttlErr != nil {
				sendFail(&request{clientID: clientID, conn: conn}, "invalid ttl")
				continue
			}
			if lockID == "" {
				sendFail(&request{clientID: clientID, conn: conn}, "invalid lock id")
				continue
			}
			if ttl > 0 && s.maxTTL > 0 && ttl > s.maxTTL {
				sendFail(&request{clientID: clientID, conn: conn}, "timeout too long")
				continue
			}
			req := &request{
				mode:           mode,
				clientID:       clientID,
				lockID:         lockID,
				conn:           conn,
				requestTimeout: s.requestTimeout,
				confirmTimeout: s.confirmTimeout,
				requestTTL:     ttl,
				grantedCh:      make(chan struct{}),
				canceledCh:     make(chan struct{}),
			}
			reqsMu.Lock()
			reqs[req.lockID] = req
			reqsMu.Unlock()
			req.onDone = func(r *request) {
				reqsMu.Lock()
				delete(reqs, r.lockID)
				reqsMu.Unlock()
			}
			s.manager.enqueue(req)
		case "CONFIRM":
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req != nil {
				s.manager.confirm(req)
			} else {
				sendFail(&request{clientID: clientID, conn: conn}, "invalid lock id")
			}
		case "RELEASE":
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			delete(reqs, arg)
			reqsMu.Unlock()
			if req != nil {
				s.manager.release(req)
			} else {
				sendFail(&request{clientID: clientID, conn: conn}, "invalid lock id")
			}
		case "REJECT":
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req != nil {
				s.manager.reject(req)
			} else {
				sendFail(&request{clientID: clientID, conn: conn}, "invalid lock id")
			}
		case "PING":
			line := fmt.Sprintf("%s PONG\n", clientID)
			_, _ = conn.Write([]byte(line))
		case "STATUS":
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req == nil || !req.confirmed {
				line := fmt.Sprintf("%s LOCKTIMEOUT %s\n", clientID, arg)
				_, _ = conn.Write([]byte(line))
				continue
			}
			if req.requestTTL > 0 && !req.expiresAt.IsZero() && time.Now().After(req.expiresAt) {
				s.manager.release(req)
				line := fmt.Sprintf("%s LOCKTIMEOUT %s\n", clientID, arg)
				_, _ = conn.Write([]byte(line))
				continue
			}
			line := fmt.Sprintf("%s LOCKOK %s\n", clientID, arg)
			_, _ = conn.Write([]byte(line))
		default:
			continue
		}
	}
}

func parseCommand(line string) (clientID, cmd, arg string, err error) {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) < 2 {
		return "", "", "", errors.New("invalid command")
	}
	clientID = fields[0]
	cmd = fields[1]
	if len(fields) > 2 {
		arg = strings.Join(fields[2:], " ")
	}
	return clientID, cmd, arg, nil
}

func parseLockAndTTL(arg string) (string, time.Duration, error) {
	fields := strings.Fields(strings.TrimSpace(arg))
	if len(fields) == 0 {
		return "", 0, errors.New("missing lock id")
	}
	lockID := fields[0]
	if len(fields) == 1 {
		return lockID, 0, nil
	}
	seconds, err := strconv.Atoi(fields[1])
	if err != nil || seconds <= 0 {
		return "", 0, errors.New("invalid ttl")
	}
	return lockID, time.Duration(seconds) * time.Second, nil
}

type lockMode int

const (
	modeRead lockMode = iota
	modeWrite
)

type request struct {
	mode           lockMode
	clientID       string
	lockID         string
	conn           net.Conn
	requestTimeout time.Duration
	confirmTimeout time.Duration
	requestTTL     time.Duration
	expiresAt      time.Time
	requestTimer   *time.Timer
	confirmTimer   *time.Timer
	granted        bool
	confirmed      bool
	done           bool
	grantedCh      chan struct{}
	canceledCh     chan struct{}
	onDone         func(*request)
}

type lockManager struct {
	mu            sync.Mutex
	policy        Policy
	queue         []*request
	activeReaders int
	activeWriter  bool
	offered       *request
}

func newLockManager(policy Policy) *lockManager {
	return &lockManager{policy: policy}
}

func (m *lockManager) enqueue(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.requestTimeout > 0 {
		req.requestTimer = time.AfterFunc(req.requestTimeout, func() {
			m.timeoutRequest(req)
		})
	}
	m.queue = append(m.queue, req)
	m.tryGrant()
}

func (m *lockManager) onConnClosed(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	if req.granted && req.confirmed {
		m.releaseLocked(req)
		return
	}
	m.cancelLocked(req)
}

func (m *lockManager) cancelLocked(req *request) {
	if req.done {
		return
	}
	stopTimer(&req.requestTimer)
	stopTimer(&req.confirmTimer)
	m.removeFromQueue(req)
	req.done = true
	if m.offered == req {
		m.offered = nil
	}
	close(req.canceledCh)
	if req.onDone != nil {
		req.onDone(req)
	}
	m.tryGrant()
}

func (m *lockManager) releaseLocked(req *request) {
	if req.done {
		return
	}
	req.done = true
	stopTimer(&req.requestTimer)
	stopTimer(&req.confirmTimer)
	if req.mode == modeRead {
		if m.activeReaders > 0 {
			m.activeReaders--
		}
	} else {
		m.activeWriter = false
	}
	if m.offered == req {
		m.offered = nil
	}
	if req.onDone != nil {
		req.onDone(req)
	}
	m.tryGrant()
}

func (m *lockManager) tryGrant() {
	if m.offered != nil {
		return
	}
	if m.activeWriter {
		return
	}

	switch m.policy {
	case ReaderPreferred:
		m.grantReadersPreferred()
	case WriterPreferred:
		m.grantWriterPreferred()
	case FIFO:
		m.grantFIFO()
	default:
		m.grantFIFO()
	}
}

func (m *lockManager) grantReadersPreferred() {
	// Grant all readers whenever no writer is active.
	if len(m.queue) == 0 {
		return
	}

	grantedAny := false
	for i := 0; i < len(m.queue); {
		req := m.queue[i]
		if req.done {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			continue
		}
		if req.mode == modeRead {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			grantedAny = true
			continue
		}
		i++
	}

	if m.activeReaders > 0 || grantedAny {
		return
	}

	// No readers active or pending; grant first writer in queue.
	for i, req := range m.queue {
		if req.mode == modeWrite {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}

func (m *lockManager) grantWriterPreferred() {
	if m.activeReaders > 0 {
		return
	}

	for i, req := range m.queue {
		if req.mode == modeWrite {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}

	// No writers pending; grant all readers.
	for i := 0; i < len(m.queue); {
		req := m.queue[i]
		if req.mode == modeRead {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			continue
		}
		i++
	}
}

func (m *lockManager) grantFIFO() {
	if m.activeReaders > 0 {
		return
	}
	if len(m.queue) == 0 {
		return
	}

	head := m.queue[0]
	if head.mode == modeWrite {
		m.grantLocked(head)
		m.queue = m.queue[1:]
		return
	}

	// Grant all consecutive readers at the front.
	count := 0
	for count < len(m.queue) && m.queue[count].mode == modeRead {
		m.grantLocked(m.queue[count])
		count++
	}
	m.queue = m.queue[count:]
}

func (m *lockManager) grantLocked(req *request) {
	if req.done {
		return
	}
	req.granted = true
	stopTimer(&req.requestTimer)
	if req.confirmTimeout > 0 {
		req.confirmTimer = time.AfterFunc(req.confirmTimeout, func() {
			m.timeoutConfirm(req)
		})
	}
	m.offered = req
	sendGrant(req)
	close(req.grantedCh)
}

func (m *lockManager) removeFromQueue(req *request) {
	for i, r := range m.queue {
		if r == req {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}

func (m *lockManager) timeoutRequest(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	if !req.granted {
		m.cancelLocked(req)
		sendFail(req, "timeout waiting for lock")
	}
}

func (m *lockManager) reject(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	if req.granted && !req.confirmed {
		m.cancelLocked(req)
		sendFail(req, "offer rejected")
	}
}

func (m *lockManager) confirm(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done || !req.granted {
		return
	}
	req.confirmed = true
	stopTimer(&req.confirmTimer)
	if req.requestTTL > 0 {
		req.expiresAt = time.Now().Add(req.requestTTL)
	}
	m.offered = nil
	if req.mode == modeRead {
		m.activeReaders++
	} else {
		m.activeWriter = true
	}
	m.tryGrant()
}

func (m *lockManager) release(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	m.releaseLocked(req)
}

func (m *lockManager) timeoutConfirm(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done || req.confirmed {
		return
	}
	m.cancelLocked(req)
	sendFail(req, "timeout waiting for confirm")
}

func sendGrant(req *request) {
	if req.conn == nil {
		return
	}
	cmd := "READLOCK"
	if req.mode == modeWrite {
		cmd = "WRITELOCK"
	}
	line := fmt.Sprintf("%s %s %s\n", req.clientID, cmd, req.lockID)
	_, _ = req.conn.Write([]byte(line))
}

func sendFail(req *request, msg string) {
	if req.conn == nil {
		return
	}
	if strings.TrimSpace(msg) == "" {
		msg = "request failed"
	}
	line := fmt.Sprintf("%s REQFAIL %s\n", req.clientID, msg)
	_, _ = req.conn.Write([]byte(line))
}

func defaultTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return 30 * time.Second
	}
	return value
}

func stopTimer(timer **time.Timer) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
}
