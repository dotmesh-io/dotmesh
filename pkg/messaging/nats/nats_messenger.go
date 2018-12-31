package nats

import (
	"context"
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/messaging"
	"github.com/dotmesh-io/dotmesh/pkg/types"

	nats "github.com/nats-io/go-nats"

	log "github.com/sirupsen/logrus"
)

var (
	RequestsSubjectTemplate = "dotmesh.events.requests.%s.%s"
	ResponseSubjectTemplate = "dotmesh.events.responses.%s.%s"
)

var _ messaging.Messenger = (*NatsMessenger)(nil)

type NatsMessenger struct {
	client        *nats.Conn
	encodedClient *nats.EncodedConn
}

func NewClient(config *Config) (*NatsMessenger, error) {
	client, err := nats.Connect(ConnectURL(config))
	if err != nil {
		return nil, err
	}

	ec, err := nats.NewEncodedConn(client, nats.GOB_ENCODER)
	if err != nil {
		return nil, err
	}

	return &NatsMessenger{
		client:        client,
		encodedClient: ec,
	}, nil
}

func (m *NatsMessenger) Publish(event *types.Event) error {
	s, err := getSubject(event)
	if err != nil {
		return err
	}
	return m.encodedClient.Publish(s, event)
}

func (m *NatsMessenger) Subscribe(ctx context.Context, q *types.SubscribeQuery) (chan *types.Event, error) {

	respCh := make(chan *types.Event)

	sub, err := m.encodedClient.Subscribe(fmt.Sprintf(RequestsSubjectTemplate, q.FilesystemID, q.RequestID), func(event *types.Event) {
		respCh <- event
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		err := sub.Unsubscribe()
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": q.FilesystemID,
				"request_id":    q.RequestID,
			}).Warn("[NatsMessenger.Subscribe] failed to unsubscribe")
		}
		close(respCh)
	}()

	return respCh, nil
}

func getSubject(event *types.Event) (string, error) {
	var subject string
	switch event.Type {
	case types.EventTypeRequest:
		subject = fmt.Sprintf(RequestsSubjectTemplate, event.FilesystemID, event.ID)
	case types.EventTypeResponse:
		subject = fmt.Sprintf(ResponseSubjectTemplate, event.FilesystemID, event.ID)
	default:
		return "", fmt.Errorf("unknown event type: %d", event.Type)
	}

	return subject, nil
}

// ConnectURL returns the ocnnection URL.
func ConnectURL(config *Config) string {
	return fmt.Sprintf("nats://%s:%d", config.Host, config.Port)
}
