package nats

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestServerStart(t *testing.T) {

	cfg := DefaultConfig()

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to configure the server: %s", err)
	}

	err = srv.Start()
	if err != nil {
		t.Errorf("failed to start: %s", err)
	}
	srv.Shutdown()
}

func TestServerStartAndConnect(t *testing.T) {

	log.SetLevel(log.DebugLevel)

	cfg := DefaultConfig()

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to configure the server: %s", err)
	}

	err = srv.Start()
	if err != nil {
		t.Errorf("failed to start: %s", err)
	}
	defer srv.Shutdown()

	client, err := NewClient(&ClientConfig{
		NatsURL:           "nats://127.0.0.1:32609",
		ConnectionTimeout: 5,
	})
	if err != nil {
		t.Errorf("failed to create a client: %s", err)
	} else {
		client.Close()
	}
}
