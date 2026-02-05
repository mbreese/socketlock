package socketlock

import "time"

type EventType string

const (
	EventClientConnected    EventType = "client_connected"
	EventClientDisconnected EventType = "client_disconnected"
	EventHello              EventType = "hello"
	EventConnected          EventType = "connected"
	EventPing               EventType = "ping"
	EventPong               EventType = "pong"
	EventLockRequested      EventType = "lock_requested"
	EventLockOffered        EventType = "lock_offered"
	EventLockConfirmed      EventType = "lock_confirmed"
	EventLockReleased       EventType = "lock_released"
	EventLockRejected       EventType = "lock_rejected"
	EventStatusOK           EventType = "status_ok"
	EventStatusTimeout      EventType = "status_timeout"
	EventRequestFailed      EventType = "request_failed"
)

type Event struct {
	Time     time.Time
	Type     EventType
	ClientID string
	LockID   string
	Mode     string
	TTL      time.Duration
	Reason   string
}
