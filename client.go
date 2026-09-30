// Package client connects to the gRPC API of a NanoPing node.
//
// A Client holds one generated client per service, so every method of the API
// is available on it, for example client.Networks.GetNetwork. Receive turns a
// stream into an iterator, and the helpers cover flows that take more than one
// call, such as joining a hub.
package client

import (
	"errors"
	"io"
	"iter"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/nanoping-labs/nanoping-api-go/v11/config"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_client"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_server"
	"github.com/nanoping-labs/nanoping-api-go/v11/logging"
	"github.com/nanoping-labs/nanoping-api-go/v11/networks"
	"github.com/nanoping-labs/nanoping-api-go/v11/pipelines"
	"github.com/nanoping-labs/nanoping-api-go/v11/resources"
)

type Client struct {
	conn *grpc.ClientConn

	Config           config.ConfigServiceClient
	HubClient        hub_client.HubClientServiceClient
	HubServer        hub_server.HubServerServiceClient
	Logging          logging.LoggingServiceClient
	Blueprints       networks.BlueprintsClient
	Networks         networks.NetworksClient
	NetworkInstances networks.NetworkInstancesClient
	Pipelines        pipelines.PipelinesServiceClient
	Resources        resources.ResourcesServiceClient
}

// Connect creates a client for the gRPC API at address, e.g. "127.0.0.1:10432".
// Without options the connection is unencrypted. The connection is made on the
// first call, so an unreachable node shows up as an Unavailable error there.
func Connect(address string, options ...grpc.DialOption) (*Client, error) {
	if len(options) == 0 {
		options = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:             conn,
		Config:           config.NewConfigServiceClient(conn),
		HubClient:        hub_client.NewHubClientServiceClient(conn),
		HubServer:        hub_server.NewHubServerServiceClient(conn),
		Logging:          logging.NewLoggingServiceClient(conn),
		Blueprints:       networks.NewBlueprintsClient(conn),
		Networks:         networks.NewNetworksClient(conn),
		NetworkInstances: networks.NewNetworkInstancesClient(conn),
		Pipelines:        pipelines.NewPipelinesServiceClient(conn),
		Resources:        resources.NewResourcesServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Receive yields the messages of a stream until it ends. A stream that fails
// yields its error once and stops; a stream that ends normally just stops.
// Stopping the loop early does not close the stream: cancel its context.
func Receive[T any](stream interface{ Recv() (T, error) }) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for {
			message, err := stream.Recv()
			if err != nil {
				if !isEndOfStream(err) {
					yield(message, err)
				}
				return
			}
			if !yield(message, nil) {
				return
			}
		}
	}
}

func isEndOfStream(err error) bool {
	return errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled
}
