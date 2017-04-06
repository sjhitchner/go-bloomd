package bloomd

import (
	"context"
	"github.com/pkg/errors"
	"gopkg.in/fatih/pool.v2"
	"net"
	"time"
)

type pooledClient struct {
	pool     Pool
	hostname string
	timeout  time.Duration
	hashKeys bool
}

func NewPooledClient(hostname string, hashKeys, timeout time.Duration, cinitialConns, maxConns int) (Client, error) {
	p, err := pool.NewChannelPool(initialConns, maxConns, func() (net.Conn, error) {
		return net.Dial("tcp", hostname)
	})

	if err != nil {
		return nil, errors.Wrap(err, "Unable to create bloomd connectionpool")
	}

	return &pooledClient{
		pool:     &fatihPool{p},
		hostname: hostname,
		timeout:  timeout,
		hashKeys: hashKeys,
	}, nil
}

// Add multi keys to the filter
func (t pooledClient) MultiSet(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.MultiSet(ctx, name, keys...)
}

// Check if key exists in filter
func (t pooledClient) Check(ctx context.Context, name Filter, key string) (bool, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Check(ctx, name, key)
}

// Clears the filter
func (t pooledClient) Clear(ctx context.Context, name Filter) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Clear(ctx, name)
}

// Closes the filter
func (t pooledClient) Close(ctx context.Context, name Filter) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Close(ctx, name)
}

// Creates new fiter
func (t pooledClient) Create(ctx context.Context, name Filter) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.CreateWithParams(ctx, name, 0, 0, false)
}

// Creates new fiter with additional params
func (t pooledClient) CreateWithParams(ctx context.Context, name Filter, capacity int, probability float64, inMemory bool) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.CreateWithParams(ctx, name, capacity, probability, inMemory)
}

// Permanently deletes filter
func (t pooledClient) Drop(ctx context.Context, name Filter) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Drop(ctx, name)
}

// Flush to disk
func (t pooledClient) Flush(ctx context.Context) error {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Flush(ctx)
}

func (t pooledClient) Info(ctx context.Context, name Filter) (map[string]string, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.Info(ctx, name)
}

// List filters
func (t pooledClient) List(ctx context.Context) (map[string]string, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.List(ctx)
}

// Checks whether multiple keys exist in the filter
func (t pooledClient) MultiCheck(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return nil, err
	}
	return conn.MultiCheck(ctx, name, keys...)
}

// Add new key to filter
func (t pooledClient) Set(ctx context.Context, name Filter, key string) (bool, error) {
	conn, err := t.pool.Get()
	if err != nil {
		return false, err
	}
	return conn.Set(ctx, name, hashKey(t.hashKeys, key))
}

func (t pooledClient) Ping() error {
	ctx := context.Background()
	_, err := t.List(ctx)
	return err
}
