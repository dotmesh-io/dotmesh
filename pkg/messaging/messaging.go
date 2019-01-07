package messaging

import (
	"context"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

type Messenger interface {
	Publish(event *types.Event) error
	Subscribe(ctx context.Context, q *types.SubscribeQuery) (respCh chan *types.Event, err error)
}

type MessagingServer interface {
	Start() error
	Shutdown()
}
