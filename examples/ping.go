package main

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GreptimeTeam/greptimedb-client-go/pkg/client"
	"GreptimeTeam/greptimedb-client-go/pkg/config"
)

func main() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := config.New("127.0.0.1:4001").AppendDialOptions(options...)
	_, err := client.New(cfg)
	if err != nil {
		fmt.Printf("err %s", err)
	}

	// TODO(yuanbohan): call real client method

}
