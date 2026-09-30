package client

import (
	"context"
	"errors"
	"iter"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nanoping-labs/nanoping-api-go/v11/hub_client"
	"github.com/nanoping-labs/nanoping-api-go/v11/hub_server"
	"github.com/nanoping-labs/nanoping-api-go/v11/logging"
	"github.com/nanoping-labs/nanoping-api-go/v11/pipelines"
)

// ErrJoinRejected is returned by JoinHub when the hub server rejected the node.
var ErrJoinRejected = errors.New("the hub server rejected the node")

// JoinHub joins the hub server at hubServerAddress, e.g.
// "http://127.0.0.1:8769", with the node hosting the API. It returns once the
// hub server approved the node, ErrJoinRejected when it was rejected, and a
// DeadlineExceeded error when nobody answered in time.
func (c *Client) JoinHub(ctx context.Context, hubServerAddress string) error {
	_, err := c.HubClient.AuthenticateByRequest(ctx, &hub_client.AuthenticateByRequestRequest{
		HubServerAddress: hubServerAddress,
	})
	if status.Code(err) == codes.Unauthenticated {
		return ErrJoinRejected
	}
	return err
}

// AnswerJoinRequests answers the nodes that ask to join the hub server hosted
// by the node hosting the API. decide is called for every request and returns
// whether the node may join. It runs until ctx is canceled or the stream fails.
func (c *Client) AnswerJoinRequests(ctx context.Context, decide func(*hub_server.AuthenticationByRequestRequest) bool) error {
	stream, err := c.HubServer.AuthenticationByRequests(ctx)
	if err != nil {
		return err
	}
	for request, err := range Receive(stream) {
		if err != nil {
			return err
		}
		err = stream.Send(&hub_server.AuthenticationByRequestAnswer{
			RequestId: request.RequestId,
			Accept:    decide(request),
		})
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// RunPipeline starts a pipeline on a node and follows the log of the new run
// from its first line. The iterator opens the log stream when the loop starts
// and yields each log message until the log stream ends, ctx is canceled or the
// loop stops, which closes the stream.
func (c *Client) RunPipeline(ctx context.Context, nodeId string, pipelineId string) (iter.Seq2[*logging.Message, error], error) {
	started, err := c.Pipelines.StartPipeline(ctx, &pipelines.StartPipelineRequest{
		NodeId:             nodeId,
		PipelineIdentifier: &pipelines.StartPipelineRequest_Id{Id: pipelineId},
	})
	if err != nil {
		return nil, err
	}

	return func(yield func(*logging.Message, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := c.Pipelines.OpenLogStream(ctx, &pipelines.OpenLogStreamRequest{
			NodeId:  nodeId,
			RunId:   started.RunId,
			Options: &logging.Options{OptionalLines: &logging.Options_Lines{Lines: math.MaxInt64}},
		})
		if err != nil {
			yield(nil, err)
			return
		}

		for batch, err := range Receive(stream) {
			if err != nil {
				yield(nil, err)
				return
			}
			for _, message := range batch.Messages {
				if !yield(message, nil) {
					return
				}
			}
		}
	}, nil
}
