package socketlock

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
)

type Server struct {
	srv *server
}

func (s *Server) Stop() error {
	if s == nil || s.srv == nil {
		return nil
	}
	return s.srv.stop()
}

// StartServer starts a primary server without creating a client connection.
func StartServer(ctx context.Context, path string, cfg LockConfig) (*Server, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("socketlock: path is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	l, err := net.Listen("unix", path)
	if err != nil {
		if isAddrInUse(err) {
			dialer := net.Dialer{}
			conn, dialErr := dialer.DialContext(ctx, "unix", path)
			if dialErr == nil {
				_ = conn.Close()
				return nil, errors.New("socketlock: server already running")
			}
			_ = os.Remove(path)
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
	return &Server{srv: srv}, nil
}
