# NanoPing API for Go

A Go client for the gRPC API of a [NanoPing](https://nanoping.com) node. It
holds the generated client of every service, with helpers for flows that take
more than one call, such as joining a hub.

This repository is generated from the NanoPing sources on every release, and
its version matches the NanoPing release it was made from. Changes made here
are overwritten by the next release.

## Install

```bash
go get github.com/nanoping-labs/nanoping-api-go/v11
```

## Use

```go
import (
    client "github.com/nanoping-labs/nanoping-api-go/v11"
    "github.com/nanoping-labs/nanoping-api-go/v11/hub_client"
)

c, err := client.Connect("127.0.0.1:10564")
if err != nil {
    return err
}
defer c.Close()

local, err := c.HubClient.GetLocalNodeId(ctx, &hub_client.GetLocalNodeIdRequest{})
```

The generated messages and service clients are in the packages under this
module, such as `github.com/nanoping-labs/nanoping-api-go/v11/hub_client`.

## Examples

`examples` holds the program the [gRPC guide](https://docs.nanoping.com/api/grpc)
is made from. It joins a node to a hub and manages pipelines on both. Start the
two nodes as the comment in `examples/main.go` describes, then run

```bash
go run ./examples --client 127.0.0.1:10432 --server 127.0.0.1:10564
```

The API is documented at https://docs.nanoping.com/api/reference.
