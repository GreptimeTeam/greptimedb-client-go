package greptimedb

import "google.golang.org/grpc"

type Config struct {
	// Address string as host:port
	Address string `json:"address"`

	// DialOptions are passed to grpc.DialContext when a new gRPC connection
	// is created.
	DialOptions []grpc.DialOption `json:"-"`
}
