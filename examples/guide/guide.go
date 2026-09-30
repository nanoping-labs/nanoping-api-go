// Package guide holds the code of the gRPC guide in the NanoPing docs. Each
// step of the guide is one function, and Run runs them in order.
package guide

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nanoping-labs/nanoping-api-go/v11"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_client"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_node"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_server"
	"github.com/nanoping-labs/nanoping-api-go/v11/logging"
	"github.com/nanoping-labs/nanoping-api-go/v11/pipelines"
)

// Addresses of the two nodes the guide uses: a client node that joins the hub,
// and the node hosting the hub server.
type Addresses struct {
	// gRPC API of the client node, e.g. "127.0.0.1:10432".
	Client string
	// gRPC API of the hub server node, e.g. "127.0.0.1:10564".
	Server string
	// HTTP address of the hub server node, e.g. "http://127.0.0.1:8769".
	ServerHttp string
}

// SetNodeInformation names the client node and gives it metadata that other
// nodes on the hub can filter on.
func SetNodeInformation(ctx context.Context, c *client.Client, out io.Writer) error {
	local, err := c.HubClient.GetLocalNodeId(ctx, &hub_client.GetLocalNodeIdRequest{})
	if err != nil {
		return err
	}

	response, err := c.HubClient.SetNode(ctx, &hub_client.SetNodeRequest{
		NodeId: local.NodeId,
		Name:   "My NanoPing Client",
		Metadata: &hub_node.NodeMetadata{
			Items: map[string]*hub_node.NodeMetadataItem{
				"type":     {Value: &hub_node.NodeMetadataItem_StringValue{StringValue: "my-type"}},
				"location": {Value: &hub_node.NodeMetadataItem_StringValue{StringValue: "Denmark, Aalborg"}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("set node information: %w", err)
	}

	fmt.Fprintf(out, "Named node %s %q\n", response.Node.Id, response.Node.Name)
	return nil
}

// WatchConnectionState prints the connection state of the client node every
// time it changes, until ctx is canceled.
func WatchConnectionState(ctx context.Context, c *client.Client, out io.Writer) error {
	stream, err := c.HubClient.StreamConnectionState(ctx, &hub_client.StreamConnectionStateRequest{})
	if err != nil {
		return err
	}

	go func() {
		for state, err := range client.Receive(stream) {
			if err != nil {
				return
			}
			fmt.Fprintf(out, "Connection state: %s\n", state.State)
		}
	}()
	return nil
}

// AcceptJoinRequests accepts every node that asks to join the hub server,
// until ctx is canceled.
func AcceptJoinRequests(ctx context.Context, server *client.Client) error {
	return server.AnswerJoinRequests(ctx, func(request *hub_server.AuthenticationByRequestRequest) bool {
		return true
	})
}

// JoinHub joins the client node to the hub server. The call waits until the
// hub server accepted or rejected the request. A node that already is part of
// the hub stays so.
func JoinHub(ctx context.Context, c *client.Client, serverHttpAddress string, out io.Writer) error {
	err := c.JoinHub(ctx, serverHttpAddress)
	if status.Code(err) == codes.AlreadyExists {
		fmt.Fprintln(out, "Already part of the hub")
		return nil
	}
	if errors.Is(err, client.ErrJoinRejected) {
		return errors.New("the hub server rejected the node")
	}
	if err != nil {
		return err
	}

	fmt.Fprintln(out, "Joined the hub")
	return nil
}

// PrintNodes prints the nodes on the hub: all of them, the ones with metadata
// "type" set to "my-type", and the ones running pipelines.
func PrintNodes(ctx context.Context, c *client.Client, out io.Writer) error {
	all, err := c.HubServer.GetNodes(ctx, &hub_server.GetNodesRequest{})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "All nodes:")
	for _, node := range all.Nodes {
		fmt.Fprintf(out, "  %s\n", node.Name)
	}

	myType, err := c.HubServer.GetNodes(ctx, &hub_server.GetNodesRequest{
		MetadataFilters: map[string]*hub_node.NodeMetadataItem{
			"type": {Value: &hub_node.NodeMetadataItem_StringValue{StringValue: "my-type"}},
		},
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "Nodes with type my-type:")
	for _, node := range myType.Nodes {
		fmt.Fprintf(out, "  %s\n", node.Name)
	}

	withPipelines, err := PipelineNodes(ctx, c)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "Nodes running pipelines:")
	for _, node := range withPipelines {
		fmt.Fprintf(out, "  %s\n", node.Name)
	}
	return nil
}

// PipelineNodes returns the nodes on the hub that run pipelines.
func PipelineNodes(ctx context.Context, c *client.Client) ([]*hub_node.Node, error) {
	response, err := c.HubServer.GetNodes(ctx, &hub_server.GetNodesRequest{
		ServiceFilters: []hub_node.NodeService{hub_node.NodeService_PIPELINES},
	})
	if err != nil {
		return nil, err
	}
	return response.Nodes, nil
}

// WatchPipelines prints an event every time a pipeline on the node is
// created, updated or deleted, until ctx is canceled. It returns once the
// stream is ready, so no change made after it returns is missed.
func WatchPipelines(ctx context.Context, c *client.Client, node *hub_node.Node, out io.Writer) error {
	stream, err := c.Pipelines.StreamPipelines(ctx, &pipelines.StreamPipelinesRequest{NodeId: node.Id})
	if err != nil {
		return err
	}
	if _, err := stream.Header(); err != nil {
		return err
	}

	go func() {
		for event, err := range client.Receive(stream) {
			if err != nil {
				return
			}
			switch e := event.Event.(type) {
			case *pipelines.StreamPipelinesEvent_Create:
				fmt.Fprintf(out, "  Event on %s: created %q\n", node.Name, e.Create.Pipeline.Name)
			case *pipelines.StreamPipelinesEvent_Update:
				fmt.Fprintf(out, "  Event on %s: updated %q\n", node.Name, e.Update.Pipeline.Name)
			case *pipelines.StreamPipelinesEvent_Delete:
				fmt.Fprintf(out, "  Event on %s: deleted %q\n", node.Name, e.Delete.Pipeline.Name)
			}
		}
	}()
	return nil
}

// ManagePipeline creates a pipeline on the node, renames it, reads it back and
// deletes it again.
func ManagePipeline(ctx context.Context, c *client.Client, nodeId string, out io.Writer) error {
	created, err := c.Pipelines.CreatePipeline(ctx, &pipelines.CreatePipelineRequest{
		NodeId:     nodeId,
		Name:       "My First Pipeline",
		JsonConfig: PipelineConfig,
		RestartPolicy: &pipelines.RestartPolicy{
			RestartPolicy: &pipelines.RestartPolicy_OnFailure{
				OnFailure: &pipelines.RestartPolicyOnFailure{MaxRestarts: 3},
			},
		},
		OptionalDefaultLoggingLevel: &pipelines.CreatePipelineRequest_DefaultLoggingLevel{
			DefaultLoggingLevel: logging.Level_DEBUG,
		},
	})
	if err != nil {
		return fmt.Errorf("create pipeline: %w", err)
	}
	pipeline := created.Pipeline
	fmt.Fprintf(out, "  Created %q\n", pipeline.Name)

	// An update replaces every setting, so send the current ones along with
	// the new name.
	updated, err := c.Pipelines.UpdatePipeline(ctx, &pipelines.UpdatePipelineRequest{
		NodeId:              nodeId,
		Id:                  pipeline.Id,
		Name:                "My Renamed Pipeline",
		JsonConfig:          pipeline.JsonConfig,
		RestartPolicy:       pipeline.RestartPolicy,
		StartupPolicy:       pipeline.StartupPolicy,
		DefaultLoggingLevel: pipeline.DefaultLoggingLevel,
		InstructionsTimeout: pipeline.InstructionsTimeout,
	})
	if err != nil {
		return fmt.Errorf("update pipeline: %w", err)
	}
	fmt.Fprintf(out, "  Renamed to %q\n", updated.Pipeline.Name)

	fetched, err := c.Pipelines.GetPipelineById(ctx, &pipelines.GetPipelineByIdRequest{NodeId: nodeId, Id: pipeline.Id})
	if err != nil {
		return fmt.Errorf("get pipeline: %w", err)
	}
	fmt.Fprintf(out, "  Read back %q\n", fetched.Pipeline.Name)

	_, err = c.Pipelines.DeletePipeline(ctx, &pipelines.DeletePipelineRequest{NodeId: nodeId, Id: pipeline.Id})
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	fmt.Fprintf(out, "  Deleted %q\n", fetched.Pipeline.Name)
	return nil
}

// Run connects to both nodes and runs every step of the guide.
func Run(ctx context.Context, addresses Addresses, out io.Writer) error {
	c, err := client.Connect(addresses.Client)
	if err != nil {
		return err
	}
	defer c.Close()

	server, err := client.Connect(addresses.Server)
	if err != nil {
		return err
	}
	defer server.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out = &lockedWriter{out: out}

	if err := WatchConnectionState(ctx, c, out); err != nil {
		return err
	}
	if err := SetNodeInformation(ctx, c, out); err != nil {
		return err
	}

	go func() { _ = AcceptJoinRequests(ctx, server) }()
	if err := JoinHub(ctx, c, addresses.ServerHttp, out); err != nil {
		return err
	}

	if err := PrintNodes(ctx, c, out); err != nil {
		return err
	}

	nodes, err := PipelineNodes(ctx, c)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		fmt.Fprintf(out, "Managing a pipeline on %s\n", node.Name)
		if err := WatchPipelines(ctx, c, node, out); err != nil {
			return err
		}
		if err := ManagePipeline(ctx, c, node.Id, out); err != nil {
			return err
		}
	}
	return nil
}
