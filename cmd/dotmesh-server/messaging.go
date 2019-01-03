package main

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/messaging/nats"

	log "github.com/sirupsen/logrus"
)

func (s *InMemoryState) initializeMessaging() error {
	log.SetLevel(log.DebugLevel)
	// start NATS server
	messagingServer, err := nats.NewServer(s.config.NatsConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err,
			"nats_config": s.config.NatsConfig,
		}).Error("[NATS] inMemoryState: failed to configure NATS server")
		return err
	}

	err = messagingServer.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err,
			"nats_config": s.config.NatsConfig,
		}).Error("[NATS] inMemoryState: failed to start NATS server")
		return err
	} else {
		log.WithFields(log.Fields{
			"host":         s.config.NatsConfig.Host,
			"port":         s.config.NatsConfig.Port,
			"http_port":    s.config.NatsConfig.HTTPPort,
			"cluster_port": s.config.NatsConfig.ClusterPort,
		}).Info("[NATS] inMemoryState: NATS server started")
	}

	s.messagingServer = messagingServer

	clientCfg := &nats.ClientConfig{
		NatsURL: fmt.Sprintf("nats://127.0.0.1:%d", nats.DefaultPort),
		// Port: nats.DefaultPort,
		// Host: "127.0.0.1",

	}

	// initializing NATS client
	messagingClient, err := nats.NewClient(clientCfg)
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err,
			"nats_config": s.config.NatsConfig,
		}).Error("[NATS] inMemoryState: failed to initialize NATS client")
		return err
	} else {
		log.Info("[NATS] inMemoryState: client initialized")
	}
	s.messenger = messagingClient

	go s.periodicMessagingClusterRoutesUpdate()
	go s.subscribeToFilesystemRequests(context.Background())

	return nil
}

func (s *InMemoryState) periodicMessagingClusterRoutesUpdate() {
	ticker := time.NewTicker(5 * time.Second)

	defer ticker.Stop()

	for range ticker.C {
		err := s.updateMessagingClusterConns()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("failed to update cluster routes")
		}
	}
}

func (s *InMemoryState) updateMessagingClusterConns() error {
	new := []string{}

	httpClient := http.DefaultClient

	for nodeID, v := range s.serverAddressesCache {
		if nodeID == s.NodeID() {
			// nothing to do
			continue
		}
		addresses := strings.Split(v, ",")
		for _, a := range addresses {

			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d", a, nats.DefaultHTTPPort), nil)

			if err != nil {
				log.WithFields(log.Fields{
					"error":   err,
					"address": a,
					"port":    nats.DefaultHTTPPort,
				}).Error("[NATS] failed to prepare request for NATS connectivity check")
				continue
			}

			_, err = httpClient.Do(req)
			if err != nil {
				log.WithFields(log.Fields{
					"error":   err,
					"address": a,
					"port":    nats.DefaultHTTPPort,
				}).Warn("[NATS] ping failed")
				continue
			}

			new = append(new, fmt.Sprintf("nats://%s:%d", a, nats.DefaultClusterPort))
		}
	}

	current := strings.Split(s.config.NatsConfig.RoutesStr, ",")
	sort.Strings(new)
	sort.Strings(current)

	if len(new) > 0 && !reflect.DeepEqual(new, current) {
		log.WithFields(log.Fields{
			"new":     new,
			"current": current,
		}).Info("[inMemoryState.updateMessagingClusterConns] NATS routes changed, updating...")

		s.config.NatsConfig.RoutesStr = strings.Join(new, ",")
		s.messagingServer.Shutdown()

		messagingServer, err := nats.NewServer(s.config.NatsConfig)
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"nats_config": s.config.NatsConfig,
			}).Error("inMemoryState: failed to configure NATS server")
			return err
		}

		err = messagingServer.Start()
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"nats_config": s.config.NatsConfig,
			}).Error("inMemoryState: failed to start NATS server")
			return err
		}

		s.messagingServer = messagingServer
	}

	return nil
}
