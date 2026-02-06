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
	onEvent        func(Event)
}

func newServer(listener net.Listener, cfg LockConfig) *server {
	return &server{
		listener:       listener,
		manager:        newLockManager(cfg.Policy, cfg.OnEvent),
		stopCh:         make(chan struct{}),
		requestTimeout: defaultTimeout(cfg.RequestTimeout),
		confirmTimeout: defaultTimeout(cfg.ConfirmTimeout),
		maxTTL:         cfg.MaxTTL,
		onEvent:        cfg.OnEvent,
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

func (s *server) emit(ev Event) {
	if s.onEvent == nil {
		return
	}
	ev.Time = time.Now()
	go s.onEvent(ev)
}

func (s *server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	reqs := make(map[string]*request)
	var reqsMu sync.Mutex
	var clientID string
	var loggedDisconnect bool

	closeCh := make(chan struct{})
	go func() {
		<-closeCh
		reqsMu.Lock()
		pending := make([]*request, 0, len(reqs))
		for _, req := range reqs {
			pending = append(pending, req)
		}
		reqsMu.Unlock()
		for _, req := range pending {
			s.manager.onConnClosed(req)
		}
		if clientID != "" && !loggedDisconnect {
			loggedDisconnect = true
			s.emit(Event{Type: EventClientDisconnected, ClientID: clientID})
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
		trimmed := strings.TrimSpace(line)
		if trimmed == cmdHello {
			if clientID == "" {
				clientID = newClientID()
			}
			s.emit(Event{Type: EventHello, ClientID: clientID})
			s.emit(Event{Type: EventClientConnected, ClientID: clientID})
			s.emit(Event{Type: EventConnected, ClientID: clientID})
			reply := fmt.Sprintf("%s %s\n", respConnected, clientID)
			_, _ = conn.Write([]byte(reply))
			continue
		}

		cid, cmd, arg, parseErr := parseCommand(line)
		if parseErr != nil {
			continue
		}
		if clientID != "" && cid != clientID {
			continue
		}
		if clientID == "" {
			continue
		}

		switch cmd {
		case cmdHello:
			s.emit(Event{Type: EventHello, ClientID: clientID})
			s.emit(Event{Type: EventClientConnected, ClientID: clientID})
			s.emit(Event{Type: EventConnected, ClientID: clientID})
			reply := fmt.Sprintf("%s %s\n", respConnected, clientID)
			_, _ = conn.Write([]byte(reply))
		case cmdEOL:
			close(closeCh)
			return
		case cmdReqRead, cmdReqWrite:
			mode := modeRead
			if cmd == cmdReqWrite {
				mode = modeWrite
			}
			lockID, ttl, ttlErr := parseLockAndTTL(arg)
			if ttlErr != nil {
				s.emit(Event{
					Type:     EventRequestFailed,
					ClientID: cid,
					LockID:   lockID,
					Mode:     modeString(mode),
					TTL:      ttl,
					Reason:   failInvalidTTL,
				})
				sendFail(&request{clientID: cid, conn: conn}, failInvalidTTL)
				continue
			}
			if lockID == "" {
				s.emit(Event{
					Type:     EventRequestFailed,
					ClientID: cid,
					Mode:     modeString(mode),
					Reason:   failInvalidLockID,
				})
				sendFail(&request{clientID: cid, conn: conn}, failInvalidLockID)
				continue
			}
			if ttl > 0 && s.maxTTL > 0 && ttl > s.maxTTL {
				s.emit(Event{
					Type:     EventRequestFailed,
					ClientID: cid,
					LockID:   lockID,
					Mode:     modeString(mode),
					TTL:      ttl,
					Reason:   failTimeoutTooLong,
				})
				sendFail(&request{clientID: cid, conn: conn}, failTimeoutTooLong)
				continue
			}
			req := &request{
				mode:           mode,
				clientID:       cid,
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
			s.emit(Event{
				Type:     EventLockRequested,
				ClientID: cid,
				LockID:   lockID,
				Mode:     modeString(mode),
				TTL:      ttl,
			})
			s.manager.enqueue(req)
		case cmdConfirm:
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req != nil {
				s.manager.confirm(req)
			} else {
				s.emit(Event{Type: EventRequestFailed, ClientID: cid, LockID: arg, Reason: failInvalidLockID})
				sendFail(&request{clientID: cid, conn: conn}, failInvalidLockID)
			}
		case cmdRelease:
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
				s.emit(Event{Type: EventRequestFailed, ClientID: cid, LockID: arg, Reason: failInvalidLockID})
				sendFail(&request{clientID: cid, conn: conn}, failInvalidLockID)
			}
		case cmdReject:
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req != nil {
				s.emit(Event{
					Type:     EventLockRejected,
					ClientID: cid,
					LockID:   arg,
					Mode:     modeString(req.mode),
					Reason:   failOfferRejected,
				})
				s.manager.reject(req)
			} else {
				s.emit(Event{Type: EventRequestFailed, ClientID: cid, LockID: arg, Reason: failInvalidLockID})
				sendFail(&request{clientID: cid, conn: conn}, failInvalidLockID)
			}
		case cmdPing:
			s.emit(Event{Type: EventPing, ClientID: cid})
			line := fmt.Sprintf("%s %s\n", cid, respPong)
			_, _ = conn.Write([]byte(line))
			s.emit(Event{Type: EventPong, ClientID: cid})
		case cmdStatus:
			if arg == "" {
				continue
			}
			reqsMu.Lock()
			req := reqs[arg]
			reqsMu.Unlock()
			if req == nil || !req.confirmed {
				s.emit(Event{Type: EventStatusTimeout, ClientID: cid, LockID: arg})
				line := fmt.Sprintf("%s %s %s\n", cid, respLockTimeout, arg)
				_, _ = conn.Write([]byte(line))
				continue
			}
			if req.requestTTL > 0 && !req.expiresAt.IsZero() && time.Now().After(req.expiresAt) {
				s.manager.release(req)
				s.emit(Event{Type: EventStatusTimeout, ClientID: cid, LockID: arg})
				line := fmt.Sprintf("%s %s %s\n", cid, respLockTimeout, arg)
				_, _ = conn.Write([]byte(line))
				continue
			}
			s.emit(Event{Type: EventStatusOK, ClientID: cid, LockID: arg})
			line := fmt.Sprintf("%s %s %s\n", cid, respLockOK, arg)
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

func modeString(mode lockMode) string {
	switch mode {
	case modeRead:
		return "read"
	case modeWrite:
		return "write"
	default:
		return "unknown"
	}
}

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
	onEvent       func(Event)
}

func newLockManager(policy Policy, onEvent func(Event)) *lockManager {
	return &lockManager{policy: policy, onEvent: onEvent}
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

func (m *lockManager) emit(ev Event) {
	if m.onEvent == nil {
		return
	}
	ev.Time = time.Now()
	go m.onEvent(ev)
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
	if len(m.queue) == 0 {
		return
	}

	// Prefer the first reader in queue.
	for i, req := range m.queue {
		if req.done {
			continue
		}
		if req.mode == modeRead {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}

	// No readers pending; grant first writer in queue.
	for i, req := range m.queue {
		if req.done {
			continue
		}
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
		if req.done {
			continue
		}
		if req.mode == modeWrite {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}

	// No writers pending; grant first reader.
	for i, req := range m.queue {
		if req.done {
			continue
		}
		if req.mode == modeRead {
			m.grantLocked(req)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
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
	if head.done {
		m.queue = m.queue[1:]
		return
	}
	m.grantLocked(head)
	m.queue = m.queue[1:]
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
	m.emit(Event{
		Type:     EventLockOffered,
		ClientID: req.clientID,
		LockID:   req.lockID,
		Mode:     modeString(req.mode),
	})
	sendGrantAsync(req)
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
		m.emit(Event{
			Type:     EventRequestFailed,
			ClientID: req.clientID,
			LockID:   req.lockID,
			Mode:     modeString(req.mode),
			Reason:   failTimeoutWaitLock,
		})
		sendFailAsync(req, failTimeoutWaitLock)
	}
}

func (m *lockManager) reject(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	if !req.granted {
		m.cancelLocked(req)
		return
	}
	if req.granted && !req.confirmed {
		m.cancelLocked(req)
		m.emit(Event{
			Type:     EventRequestFailed,
			ClientID: req.clientID,
			LockID:   req.lockID,
			Mode:     modeString(req.mode),
			Reason:   failOfferRejected,
		})
		sendFailAsync(req, failOfferRejected)
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
	m.emit(Event{
		Type:     EventLockConfirmed,
		ClientID: req.clientID,
		LockID:   req.lockID,
		Mode:     modeString(req.mode),
	})
	m.tryGrant()
}

func (m *lockManager) release(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done {
		return
	}
	m.releaseLocked(req)
	m.emit(Event{
		Type:     EventLockReleased,
		ClientID: req.clientID,
		LockID:   req.lockID,
		Mode:     modeString(req.mode),
	})
}

func (m *lockManager) timeoutConfirm(req *request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.done || req.confirmed {
		return
	}
	m.cancelLocked(req)
	m.emit(Event{
		Type:     EventRequestFailed,
		ClientID: req.clientID,
		LockID:   req.lockID,
		Mode:     modeString(req.mode),
		Reason:   failTimeoutWaitConfirm,
	})
	sendFailAsync(req, failTimeoutWaitConfirm)
}

func sendGrantAsync(req *request) {
	if req.conn == nil {
		return
	}
	cmd := respReadLock
	if req.mode == modeWrite {
		cmd = respWriteLock
	}
	line := fmt.Sprintf("%s %s %s\n", req.clientID, cmd, req.lockID)
	go func(conn net.Conn, payload string) {
		_, _ = conn.Write([]byte(payload))
	}(req.conn, line)
}

func sendFail(req *request, msg string) {
	if req.conn == nil {
		return
	}
	if strings.TrimSpace(msg) == "" {
		msg = failRequestFailed
	}
	line := fmt.Sprintf("%s %s %s\n", req.clientID, respReqFail, msg)
	_, _ = req.conn.Write([]byte(line))
}

func sendFailAsync(req *request, msg string) {
	if req.conn == nil {
		return
	}
	if strings.TrimSpace(msg) == "" {
		msg = failRequestFailed
	}
	line := fmt.Sprintf("%s %s %s\n", req.clientID, respReqFail, msg)
	go func(conn net.Conn, payload string) {
		_, _ = conn.Write([]byte(payload))
	}(req.conn, line)
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
