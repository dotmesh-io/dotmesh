package nats

import (
	"context"
	"fmt"
	"time"

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
	config        *ClientConfig
	client        *nats.Conn
	encodedClient *nats.EncodedConn
}

type ClientConfig struct {
	NatsURL           string
	ConnectionTimeout int64
}

func NewClient(config *ClientConfig) (*NatsMessenger, error) {

	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 30
	}

	if config.NatsURL == "" {
		return nil, fmt.Errorf("NATS client connection URL not set")
	}
	// DefaultURL = "nats://localhost:4222"

	// "nats://127.0.0.1:32609"
	client, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, err
	}

	ec, err := nats.NewEncodedConn(client, nats.GOB_ENCODER)
	if err != nil {
		return nil, err
	}

	// testing
	mc := &NatsMessenger{
		client:        client,
		encodedClient: ec,
		config:        config,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.ConnectionTimeout)*time.Second)
	defer cancel()
	err = mc.ensureConnected(ctx)
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func (m *NatsMessenger) Close() {
	m.client.Close()
}

func (m *NatsMessenger) ensureConnected(ctx context.Context) error {
	subject := "dotmesh.testsubject"
	got := make(chan bool)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("deadline exceeded")
		case <-got:
			log.Info("messenger: initial health check for NATS completed")
			return nil
		default:

			sub, err := m.client.Subscribe(subject, func(msg *nats.Msg) {
				got <- true
			})
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"url":   m.config.NatsURL,
				}).Warn("messenger: can't subscribe to a test event")
				time.Sleep(500 * time.Millisecond)
				continue
			}

			err = m.client.Publish(subject, []byte("foo"))
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"url":   m.config.NatsURL,
				}).Warn("messenger: can't publish a test event")
				sub.Unsubscribe()
				time.Sleep(500 * time.Millisecond)

				continue
			}
			time.Sleep(500 * time.Millisecond)
			err = sub.Unsubscribe()
			if err != nil {
				log.WithError(err).Warn("messenger: failed to unsubscribe from a test event subscription")
			}

		}
	}
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

	sub, err := m.encodedClient.Subscribe(fmt.Sprintf(RequestsSubjectTemplate, q.GetFilesystemID(), q.GetRequestID()), func(event *types.Event) {
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
				"filesystem_id": q.GetFilesystemID(),
				"request_id":    q.GetRequestID(),
			}).Warn("[NatsMessenger.Subscribe] failed to unsubscribe")
		}
		close(respCh)
	}()

	return respCh, nil
}

func getSubject(event *types.Event) (string, error) {
	if event.FilesystemID == "" {
		return "", fmt.Errorf("event FilesystemID cannot be empty")
	}
	if event.ID == "" {
		return "", fmt.Errorf("event ID cannot be empty")
	}
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
