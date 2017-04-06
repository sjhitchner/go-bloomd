package bloomd

import (
	"context"
	"net"
	"time"
)

type client struct {
	hostname    string
	timeout     time.Duration
	maxAttempts int
	hashKeys    bool
}

func NewClient(hostname string, hashKeys bool, timeout time.Duration) (Client, error) {
	return &client{
		hostname:    hostname,
		timeout:     timeout,
		hashKeys:    hashKeys,
		maxAttempts: 3,
	}, nil
}

// Add multi keys to the filter
func (t client) MultiSet(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.MultiSet(ctx, name, keys...)
}

// Check if key exists in filter
func (t client) Check(ctx context.Context, name Filter, key string) (bool, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Check(ctx, name, key)
}

// Clears the filter
func (t client) Clear(ctx context.Context, name Filter) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Clear(ctx, name)
}

// Closes the filter
func (t client) Close(ctx context.Context, name Filter) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Close(ctx, name)
}

// Creates new fiter
func (t client) Create(ctx context.Context, name Filter) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.CreateWithParams(ctx, name, 0, 0, false)
}

// Creates new fiter with additional params
func (t client) CreateWithParams(ctx context.Context, name Filter, capacity int, probability float64, inMemory bool) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.CreateWithParams(ctx, name, capacity, probability, inMemory)
}

// Permanently deletes filter
func (t client) Drop(ctx context.Context, name Filter) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Drop(ctx, name)
}

// Flush to disk
func (t client) Flush(ctx context.Context) error {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Flush(ctx)
}

func (t client) Info(ctx context.Context, name Filter) (map[string]string, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.Info(ctx, name)
}

// List filters
func (t client) List(ctx context.Context) (map[string]string, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.List(ctx)
}

// Checks whether multiple keys exist in the filter
func (t client) MultiCheck(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return nil, err
	}
	return conn.MultiCheck(ctx, name, keys...)
}

// Add new key to filter
func (t client) Set(ctx context.Context, name Filter, key string) (bool, error) {
	conn, err := newConnection(t.maxAttempts)
	if err != nil {
		return false, err
	}
	return conn.Set(ctx, name, key)
}

func newConnection(maxAttempts int) (Connection, error) {
	attempted := 0

	var conn net.Conn
	var err error
	for attempted < maxAttempts {
		conn, err = net.Dial("tcp", t.hostname)
		if err == nil {
			conn.SetReadDeadline(time.Now().Add(time.Second))
			return &connection{conn}, nil
		}
		attempted++
	}

	return nil, errors.Wrap(err, "bloomd: unable to establish a connection")
}

func (t client) Ping() error {
	ctx := context.Background()
	_, err := t.List(ctx)
	return err
}
