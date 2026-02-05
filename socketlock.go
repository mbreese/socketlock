package socketlock

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Policy defines how the primary grants locks when there is contention.
type Policy int

const (
	ReaderPreferred Policy = iota
	WriterPreferred
	FIFO
)

func (p Policy) String() string {
	switch p {
	case ReaderPreferred:
		return "ReaderPreferred"
	case WriterPreferred:
		return "WriterPreferred"
	case FIFO:
		return "FIFO"
	default:
		return fmt.Sprintf("Policy(%d)", int(p))
	}
}

// LockConfig configures a primary instance.
type LockConfig struct {
	Policy         Policy
	RequestTimeout time.Duration
	ConfirmTimeout time.Duration
	MaxTTL         time.Duration
	StatusInterval time.Duration
	Heartbeat      time.Duration
	ClientID       string
}

// Client connects to a socketlock primary and acquires locks.
type Client struct {
	path       string
	clientID   string
	isPrimary  bool
	server     *server
	conn       net.Conn
	readerDone chan struct{}
	heartbeat  time.Duration
	hbStop     chan struct{}

	statusInterval time.Duration

	mu           sync.Mutex
	pending      []*pendingRequest
	inFlight     bool
	closed       bool
	lastActivity time.Time
	cond         *sync.Cond
	active       *lockState
}

// Connect returns a client connected to the primary at path.
// If the primary is not running, this client becomes the primary.
// The LockConfig is only applied when this process becomes primary.
func Connect(ctx context.Context, path string, cfg LockConfig) (*Client, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("socketlock: path is required")
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err == nil {
		conn.Close()
		return connectClient(ctx, path, cfg, nil)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	sleepReconnectJitter()
	l, err := net.Listen("unix", path)
	if err != nil {
		if isAddrInUse(err) {
			// Another process may have won the race. Try dialing once more.
			conn, dialErr := dialer.DialContext(ctx, "unix", path)
			if dialErr == nil {
				conn.Close()
				return &Client{path: path}, nil
			}
			// Stale socket; remove and retry.
			_ = os.Remove(path)
			sleepReconnectJitter()
			l, err = net.Listen("unix", path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	srv := newServer(l, cfg)
	srv.start()

	return connectClient(ctx, path, cfg, srv)
}

// Close stops the primary server if this client started it.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.waitForLocks()
	c.sendEOL()
	if c.server == nil {
		return c.closeConn()
	}
	if err := c.closeConn(); err != nil {
		return err
	}
	return c.server.stop()
}

// AcquireWrite acquires a write lock.
func (c *Client) AcquireWrite(ctx context.Context) (*Lock, error) {
	return c.acquire(ctx, "REQWRITE")
}

func (c *Client) AcquireRead(ctx context.Context) (*Lock, error) {
	return c.acquire(ctx, "REQREAD")
}

func (c *Client) acquire(ctx context.Context, cmd string) (*Lock, error) {
	if c == nil {
		return nil, errors.New("socketlock: client is nil")
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	ttl := ttlFromContext(ctx)
	req := &pendingRequest{
		cmd:    cmd,
		lockID: newLockID(),
		result: make(chan acquireResult, 1),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("socketlock: client is closed")
	}
	if c.active != nil {
		if c.active.expired.Load() || c.active.released {
			c.mu.Unlock()
			return nil, errors.New("socketlock: lock expired")
		}
		if cmd == "REQWRITE" && c.active.mode == modeRead {
			c.mu.Unlock()
			return nil, errors.New("socketlock: write requested while read lock held")
		}
		c.active.count++
		lock := &Lock{client: c, state: c.active}
		c.mu.Unlock()
		return lock, nil
	}
	if c.inFlight {
		c.mu.Unlock()
		return nil, errors.New("socketlock: multiple lock requests")
	}
	c.inFlight = true
	c.pending = append(c.pending, req)
	if ttl > 0 {
		seconds := int64(math.Ceil(ttl.Seconds()))
		if seconds <= 0 {
			seconds = 1
		}
		if err := c.writeLineLocked(fmt.Sprintf("%s %s %s %d\n", c.clientID, cmd, req.lockID, seconds)); err != nil {
			c.removePending(req)
			c.inFlight = false
			c.mu.Unlock()
			return nil, err
		}
		callRequestSentHook()
	} else {
		if err := c.writeLineLocked(fmt.Sprintf("%s %s %s\n", c.clientID, cmd, req.lockID)); err != nil {
			c.removePending(req)
			c.inFlight = false
			c.mu.Unlock()
			return nil, err
		}
		callRequestSentHook()
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.removePending(req)
		c.inFlight = false
		if c.cond != nil {
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		if req.lockID != "" {
			_ = c.sendReject(req)
		}
		return nil, ctx.Err()
	case res := <-req.result:
		c.mu.Lock()
		c.inFlight = false
		if c.cond != nil {
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		if res.err != nil {
			return nil, res.err
		}
		if err := c.sendConfirm(res.lockID); err != nil {
			return nil, err
		}
		state := &lockState{
			mode:       modeRead,
			lockID:     res.lockID,
			count:      1,
			statusStop: make(chan struct{}),
		}
		if cmd == "REQWRITE" {
			state.mode = modeWrite
		}
		c.mu.Lock()
		c.active = state
		if c.cond != nil {
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		lock := &Lock{client: c, state: state}
		lock.startStatusLoop()
		return lock, nil
	}
}

func (c *Client) sendConfirm(lockID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("socketlock: client is closed")
	}
	return c.writeLineLocked(fmt.Sprintf("%s CONFIRM %s\n", c.clientID, lockID))
}

func (c *Client) sendReject(req *pendingRequest) error {
	if req == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("socketlock: client is closed")
	}
	if req.lockID == "" {
		return errors.New("socketlock: reject requires lock id")
	}
	return c.writeLineLocked(fmt.Sprintf("%s REJECT %s\n", c.clientID, req.lockID))
}

func (c *Client) sendEOL() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.conn == nil {
		return
	}
	_, _ = c.conn.Write([]byte(fmt.Sprintf("%s EOL\n", c.clientID)))
}

// Lock represents an acquired lock. Release sends a RELEASE command.
type Lock struct {
	client *Client
	state  *lockState
	once   sync.Once
}

// Release relinquishes the lock.
func (l *Lock) Release() error {
	if l == nil || l.client == nil || l.state == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		err = l.client.releaseState(l.state)
	})
	return err
}

// Expired reports whether the lock is no longer active per STATUS checks.
func (l *Lock) Expired() bool {
	if l == nil || l.state == nil {
		return true
	}
	return l.state.expired.Load()
}

func (c *Client) releaseState(state *lockState) error {
	if state == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	if c.active != state {
		return nil
	}
	if state.count > 1 {
		state.count--
		return nil
	}
	state.count = 0
	state.released = true
	close(state.statusStop)
	c.active = nil
	if c.cond != nil {
		c.cond.Broadcast()
	}
	return c.writeLineLocked(fmt.Sprintf("%s RELEASE %s\n", c.clientID, state.lockID))
}

func (c *Client) writeLineLocked(line string) error {
	if c.conn == nil {
		return errors.New("socketlock: connection not initialized")
	}
	c.lastActivity = time.Now()
	_, err := c.conn.Write([]byte(line))
	return err
}

type pendingRequest struct {
	cmd    string
	result chan acquireResult
	lockID string
}

type acquireResult struct {
	lockID string
	err    error
}

type lockState struct {
	mode       lockMode
	lockID     string
	count      int
	statusStop chan struct{}
	expired    atomic.Bool
	released   bool
}

func connectClient(ctx context.Context, path string, cfg LockConfig, srv *server) (*Client, error) {
	clientID := cfg.ClientID
	if strings.TrimSpace(clientID) == "" {
		clientID = newClientID()
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		if srv != nil {
			_ = srv.stop()
		}
		return nil, err
	}
	c := &Client{
		path:           path,
		clientID:       clientID,
		isPrimary:      srv != nil,
		server:         srv,
		conn:           conn,
		readerDone:     make(chan struct{}),
		heartbeat:      defaultHeartbeat(cfg.Heartbeat),
		hbStop:         make(chan struct{}),
		statusInterval: defaultStatusInterval(cfg.StatusInterval),
		pending:        make([]*pendingRequest, 0, 4),
	}
	c.cond = sync.NewCond(&c.mu)
	c.touchLocked()
	if err := c.sendHello(); err != nil {
		_ = c.closeConn()
		return nil, err
	}
	go c.readLoop()
	go c.heartbeatLoop()
	return c, nil
}

func (c *Client) sendHello() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("socketlock: client is closed")
	}
	if err := c.writeLineLocked(fmt.Sprintf("%s HELLO\n", c.clientID)); err != nil {
		return err
	}
	return nil
}

func (c *Client) readLoop() {
	defer close(c.readerDone)
	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			c.failAllPending(err)
			return
		}
		c.mu.Lock()
		c.lastActivity = time.Now()
		c.mu.Unlock()
		c.handleLine(line)
	}
}

func (c *Client) handleLine(line string) {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) < 2 {
		return
	}
	if fields[0] != c.clientID {
		return
	}
	switch fields[1] {
	case "REQFAIL":
		req := c.popPending()
		if req == nil {
			return
		}
		c.mu.Lock()
		c.inFlight = false
		if c.cond != nil {
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		msg := ""
		if len(fields) > 2 {
			msg = strings.Join(fields[2:], " ")
		}
		if msg == "" {
			msg = "request failed"
		}
		req.result <- acquireResult{err: errors.New("socketlock: " + msg)}
	case "READLOCK", "WRITELOCK":
		if len(fields) < 3 {
			return
		}
		req := c.popPending()
		if req == nil {
			return
		}
		c.mu.Lock()
		c.inFlight = false
		if c.cond != nil {
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		req.lockID = fields[2]
		req.result <- acquireResult{lockID: fields[2]}
	case "LOCKOK":
		if len(fields) < 3 {
			return
		}
		// no-op, status is healthy
	case "LOCKTIMEOUT":
		if len(fields) < 3 {
			return
		}
		c.mu.Lock()
		state := c.active
		if state != nil && state.lockID == fields[2] {
			state.expired.Store(true)
			state.released = true
			close(state.statusStop)
			c.active = nil
			if c.cond != nil {
				c.cond.Broadcast()
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) popPending() *pendingRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	req := c.pending[0]
	c.pending = c.pending[1:]
	return req
}

func (c *Client) removePending(target *pendingRequest) {
	if target == nil {
		return
	}
	for i, req := range c.pending {
		if req == target {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return
		}
	}
}

func (c *Client) failAllPending(err error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = nil
	c.inFlight = false
	if c.cond != nil {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	for _, req := range pending {
		req.result <- acquireResult{err: err}
	}
}

func (c *Client) closeConn() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	if c.active != nil && conn != nil {
		_, _ = conn.Write([]byte(fmt.Sprintf("%s RELEASE %s\n", c.clientID, c.active.lockID)))
		c.active.released = true
		close(c.active.statusStop)
		c.active = nil
	}
	c.conn = nil
	close(c.hbStop)
	if c.cond != nil {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	<-c.readerDone
	return nil
}

var clientSeq atomic.Uint64

func newClientID() string {
	seq := clientSeq.Add(1)
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("c%04x-%d", hex.EncodeToString(buf), seq)
}

func newLockID() string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("l%s", hex.EncodeToString(buf))
}

type ttlKey struct{}

// WithTTL attaches a per-request TTL used for REQREAD/REQWRITE.
func WithTTL(ctx context.Context, ttl time.Duration) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ttlKey{}, ttl)
}

func ttlFromContext(ctx context.Context) time.Duration {
	if ctx == nil {
		return 0
	}
	if v := ctx.Value(ttlKey{}); v != nil {
		if ttl, ok := v.(time.Duration); ok {
			return ttl
		}
	}
	return 0
}

func defaultStatusInterval(value time.Duration) time.Duration {
	if value <= 0 {
		return 10 * time.Second
	}
	return value
}

func (l *Lock) startStatusLoop() {
	interval := l.client.statusInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-l.state.statusStop:
				return
			case <-ticker.C:
				if l.state.expired.Load() || l.state.released {
					return
				}
				l.client.mu.Lock()
				if l.client.closed {
					l.client.mu.Unlock()
					return
				}
				_ = l.client.writeLineLocked(fmt.Sprintf("%s STATUS %s\n", l.client.clientID, l.state.lockID))
				l.client.mu.Unlock()
			}
		}
	}()
}

func defaultHeartbeat(value time.Duration) time.Duration {
	if value <= 0 {
		return 30 * time.Second
	}
	return value
}

func (c *Client) waitForLocks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cond == nil {
		return
	}
	for c.active != nil {
		c.cond.Wait()
	}
}

func sleepReconnectJitter() {
	delay := time.Duration(100+mrand.Intn(2001)) * time.Millisecond
	time.Sleep(delay)
}

func (c *Client) touchLocked() {
	c.lastActivity = time.Now()
}

func (c *Client) heartbeatLoop() {
	if c.heartbeat <= 0 {
		return
	}
	interval := c.heartbeat / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.hbStop:
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			idle := time.Since(c.lastActivity)
			if idle < c.heartbeat {
				c.mu.Unlock()
				continue
			}
			_ = c.writeLineLocked(fmt.Sprintf("%s PING\n", c.clientID))
			c.mu.Unlock()
		}
	}
}

func isAddrInUse(err error) bool {
	return strings.Contains(err.Error(), "address already in use") ||
		strings.Contains(err.Error(), "bind: address already in use") ||
		strings.Contains(err.Error(), "EADDRINUSE")
}

// Default timeout used by tests to avoid hangs.
var testTimeout = 5 * time.Second

// testRequestSentHook is set by tests to observe when a request is sent.
var testRequestSentHook func()

func callRequestSentHook() {
	if testRequestSentHook != nil {
		testRequestSentHook()
	}
}
