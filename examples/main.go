// Runs the gRPC guide of the NanoPing docs against two nodes.
//
// Start the client node with
//
//	np up --grpc 127.0.0.1:10432
//
// and the hub server node with `np -c config.yaml up`, where config.yaml is
//
//	node:
//	  name: My Server
//	http:
//	  address: 127.0.0.1:8769
//	hubServer:
//	  authenticationByRequest:
//	    timeout: 5m
//	grpcBridge:
//	  address: 127.0.0.1:10564
//	pipelines: {}
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/nanoping-labs/nanoping-api-go/v11/examples/guide"
)

func main() {
	var addresses guide.Addresses
	flag.StringVar(&addresses.Client, "client", "127.0.0.1:10432", "gRPC API of the client node")
	flag.StringVar(&addresses.Server, "server", "127.0.0.1:10564", "gRPC API of the hub server node")
	flag.StringVar(&addresses.ServerHttp, "server-http", "http://127.0.0.1:8769", "HTTP address of the hub server node")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := guide.Run(ctx, addresses, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
