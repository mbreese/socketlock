package socketlock

const (
	cmdHello    = "HELLO"
	cmdReqRead  = "REQREAD"
	cmdReqWrite = "REQWRITE"
	cmdConfirm  = "CONFIRM"
	cmdReject   = "REJECT"
	cmdRelease  = "RELEASE"
	cmdStatus   = "STATUS"
	cmdPing     = "PING"
	cmdEOL      = "EOL"

	respConnected   = "CONNECTED"
	respReadLock    = "READLOCK"
	respWriteLock   = "WRITELOCK"
	respReqFail     = "REQFAIL"
	respLockOK      = "LOCKOK"
	respLockTimeout = "LOCKTIMEOUT"
	respPong        = "PONG"
)

const (
	failTimeoutWaitLock    = "timeout waiting for lock"
	failTimeoutWaitConfirm = "timeout waiting for confirm"
	failTimeoutTooLong     = "timeout too long"
	failInvalidTTL         = "invalid ttl"
	failInvalidLockID      = "invalid lock id"
	failOfferRejected      = "offer rejected"
	failRequestFailed      = "request failed"
)
