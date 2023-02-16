package greptimedb

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/flight"
)

type Client struct {
	Config Config
	Client flight.Client
}

// New will create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func New(ctx context.Context, cfg Config) (*Client, error) {

	return nil, errors.New("")
}

// Write ...
func (*Client) Write() error {
	return errors.New("")
}

// Read ...
func (*Client) Read() error {
	return errors.New("")
}
