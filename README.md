# socketlock

Distributed read/write locking over a Unix domain socket (auto-configuring single-host client/server).

This is not a strict serverless model. A server exists at any moment, but any process can become the server (primary) if the current primary disappears.
All protocol actions are initiated by clients; the server only responds.

## Protocol

Each command is a single line, terminated by `\n`.

**Client -> Server**
- `<client-id> HELLO\n`
- `<client-id> REQREAD [seconds]\n`
- `<client-id> REQWRITE [seconds]\n`
- `<client-id> CONFIRM <lock-id>\n`
- `<client-id> REJECT <lock-id>\n`
- `<client-id> RELEASE <lock-id>\n`
- `<client-id> STATUS <lock-id>\n`
- `<client-id> PING\n`
- `<client-id> EOL\n`

**Server -> Client**
- `<client-id> CONNECTED\n`
- `<client-id> READLOCK <lock-id>\n` (read lock offered)
- `<client-id> WRITELOCK <lock-id>\n` (write lock offered)
- `<client-id> REQFAIL <message>\n` (request failed)
- `<client-id> LOCKOK <lock-id>\n`
- `<client-id> LOCKTIMEOUT <lock-id>\n`
- `<client-id> PONG\n`

## Semantics

- Each process/node has an auto-generated instance ID (`client-id`) and includes it in every request (override via `LockConfig.ClientID`).
- Server replies always echo the **client's** `client-id` (not the primary's).
- A request has a server-side time limit for waiting in the queue (default `30s`, configurable via `LockConfig.RequestTimeout`).
- If a lock cannot be granted within the time limit, the server responds with `REQFAIL timeout waiting for lock`.
- `AcquireRead/AcquireWrite` return an error that includes the `REQFAIL` message.
- After receiving `READLOCK` or `WRITELOCK`, the client **must** confirm with `CONFIRM <lock-id>` before the lock becomes active.
- If a granted lock is not confirmed within the confirmation time limit (default `30s`, configurable via `LockConfig.ConfirmTimeout`), the server aborts it.
- Once a lock is offered, **all other requests are blocked** until the offer is confirmed or times out.
- Requested TTL (`seconds`) is optional. If provided and greater than `MaxTTL`, the server responds with `REQFAIL timeout too long`.
- The server does not initiate messages for TTL expiry. Clients must send `STATUS <lock-id>` every `StatusInterval` (default `10s`).
- Server responds `LOCKOK <lock-id>` or `LOCKTIMEOUT <lock-id>` if the lock is no longer active.
- To release a lock, the client sends `RELEASE <lock-id>`.
- If a client rejects an offered lock (typically after local timeout), it sends `REJECT <lock-id>` and the server responds with `REQFAIL`.
- When disconnecting, the client sends `EOL` and closes the connection. The server does not acknowledge `EOL`.
- A client sends `EOL` only after any active lock is released. `Close()` blocks until active locks are released.
- Clients send `PING` every 30 seconds of inactivity (configurable via `LockConfig.Heartbeat`). The server responds with `PONG`.
- Clients may track internal lock usage with a counter; when it reaches zero, they release the socketlock lock.
- Clients may close their connection at any time. Closing the connection releases any active lock(s) on the server.
- If the primary server closes, it does **not** notify clients.
- If a client connection fails, it attempts to become the new primary by binding the socket path.
- After receiving `EOL`, clients wait a random delay between 100ms and 2100ms before attempting to reconnect or become primary.

`REQFAIL` message reasons (current):
- `timeout waiting for lock`
- `timeout waiting for confirm`
- `timeout too long`
- `invalid ttl`
- `invalid lock id`
- `offer rejected`

## API Shape (planned)

- `Connect(ctx, path, LockConfig)`
- `AcquireRead(ctx) -> Lock`
- `AcquireWrite(ctx) -> Lock`
- `Lock.Release()`
- `Close()`

Optional helper:
- `WithTTL(ctx, ttl)` to request a per-lock TTL on `AcquireRead/AcquireWrite`.

Notes:
- Only one `Acquire*` may be in flight per client connection. A concurrent call returns `error("socketlock: multiple lock requests")`.
- `AcquireRead/AcquireWrite` surface `REQFAIL` messages in the returned error.

Example:
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mbreese/socketlock"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := socketlock.Connect(ctx, "/tmp/locks.sock", socketlock.LockConfig{
		Policy:         socketlock.WriterPreferred,
		RequestTimeout: 2 * time.Second,
		ConfirmTimeout: 2 * time.Second,
		MaxTTL:         30 * time.Second,
		StatusInterval: 10 * time.Second,
		Heartbeat:      30 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	lock, err := client.AcquireRead(socketlock.WithTTL(ctx, 10*time.Second))
	if err != nil {
		// REQFAIL messages show up here, e.g. "socketlock: timeout too long"
		fmt.Println("acquire failed:", err)
		return
	}
	defer lock.Release()

	fmt.Println("lock acquired")
}
```

`LockConfig` fields:
- `Policy` (`ReaderPreferred`, `WriterPreferred`, `FIFO`)
- `RequestTimeout` (time to receive an offer)
- `ConfirmTimeout` (time to confirm an offer)
- `MaxTTL` (maximum requested TTL)
- `StatusInterval` (STATUS interval; default `10s`)
- `Heartbeat` (idle heartbeat interval; default `30s`)
- `ClientID` (optional override for instance ID)
