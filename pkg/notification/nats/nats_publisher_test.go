package nats

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/notification"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/gnatsd/test"
)

var defaultNatsTestOptions = &server.Options{
	Host:           "127.0.0.1",
	Port:           31045,
	NoLog:          true,
	NoSigs:         true,
	MaxControlLine: 256,
}

func TestMain(m *testing.M) {
	// starting NATS server
	test.RunServer(defaultNatsTestOptions)

	// running tests
	retCode := m.Run()
	os.Exit(retCode)
}

func TestConfigure(t *testing.T) {
	os.Setenv(EnvNatsURL, fmt.Sprintf("nats://127.0.0.1:%d", defaultNatsTestOptions.Port))

	p := &publisher{}

	configured, err := p.Configure(&notification.Config{})
	if err != nil {
		t.Fatalf("failed to configure: %s", err)
	}

	defer p.client.Close()

	if !configured {
		t.Fatalf("expected addon to be configured")
	}
}
func TestPublishCommit(t *testing.T) {
	os.Setenv(EnvNatsURL, fmt.Sprintf("nats://127.0.0.1:%d", defaultNatsTestOptions.Port))

	p := &publisher{}

	configured, err := p.Configure(&notification.Config{})
	if err != nil {
		t.Fatalf("failed to configure: %s", err)
	}

	defer p.client.Close()

	if !configured {
		t.Fatalf("expected addon to be configured")
	}

	foundCh := make(chan bool, 1)

	expectedCommit := "1234"

	sub, err := p.encodedClient.Subscribe(p.subject, func(notification *types.CommitNotification) {
		if notification.CommitId == expectedCommit {
			foundCh <- true
		}
	})
	if err != nil {
		t.Fatalf("failed to create subscription for incoming tasks: %s", err)
	}
	defer sub.Unsubscribe()

	err = p.PublishCommit(&types.CommitNotification{
		CommitId: expectedCommit,
	})

	if err != nil {
		t.Errorf("failed to publish commit: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Errorf("didn't get the commit notification, deadline exceeded")
	case <-foundCh:
		// ok
		return
	}
}
