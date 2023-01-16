package session

import (
	"fmt"
	"strings"
	"time"

	zookeeper "github.com/Shopify/gozk"
)

type SessionOpts struct {
	recvTimeout time.Duration
	logger      stdLogger
	clientID    *zookeeper.ClientId
	servers     []string
	dnsRefresh  time.Duration
}

// Create initializes a new session with the settings in s by connecting to the
// configured servers and waiting until a session is established.
func (s SessionOpts) Create() (*ZKSession, error) {
	var conn *zookeeper.Conn
	var events <-chan zookeeper.Event
	var err error

	if len(s.servers) == 0 {
		return nil, fmt.Errorf("no zookeeper servers specified")
	}

	servers := strings.Join(s.servers, ",")
	if s.clientID == nil {
		conn, events, err = zookeeper.Dial(servers, s.recvTimeout)
	} else {
		conn, events, err = zookeeper.Redial(servers, s.recvTimeout, s.clientID)
	}

	if err != nil {
		return nil, err
	}

	conn.SetServersResolutionDelay(s.dnsRefresh)

	session := &ZKSession{
		opts:          s,
		conn:          conn,
		events:        events,
		subscriptions: make([]chan<- ZKSessionEvent, 0),
		log:           s.logger,
	}

	err = waitForConnection(events)
	if err != nil {
		_ = session.conn.Close()
		return nil, fmt.Errorf("waiting for initial connection: %w", err)
	}

	return session, nil
}

func waitForConnection(events <-chan zookeeper.Event) error {
	for {
		select {
		case event := <-events:
			switch event.State {
			case zookeeper.STATE_AUTH_FAILED, zookeeper.STATE_EXPIRED_SESSION, zookeeper.STATE_CLOSED:
				return ErrZKSessionNotConnected
			case zookeeper.STATE_CONNECTED:
				return nil
			}
		case <-time.After(5 * time.Second):
			return ErrZKSessionNotConnected
		}
	}
}

type SessionOpt func(SessionOpts) SessionOpts

// WithRecvTimeout creates a session with the given timeout.
func WithRecvTimeout(timeout time.Duration) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.recvTimeout = timeout
		return so
	}
}

// WithLogger creates a session with the given logger.
func WithLogger(logger stdLogger) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		// Maintain backwards compatibility
		if logger == nil {
			logger = &nullLogger{}
		}
		so.logger = logger
		return so
	}
}

// WithZookeepers creates a session with the given zookeeper hosts.
func WithZookeepers(zookeepers []string) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.servers = zookeepers
		return so
	}
}

// WithZookeeperClientID creates a session with the given client ID.
func WithZookeeperClientID(id *zookeeper.ClientId) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.clientID = id
		return so
	}
}

// WithZookeeperClientID creates a session with periodic DNS refresh enabled.
func WithDNSRefresh(duration time.Duration) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.dnsRefresh = duration
		return so
	}
}
