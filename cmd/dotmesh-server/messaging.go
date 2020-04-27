package main

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/messaging/nats"

	log "github.com/sirupsen/logrus"
)

// initializeMessaging - sets up server, client and starts periodic route synchronization from the
// known server list.
func (s *InMemoryState) initializeMessaging() error {
	// start NATS server
	messagingServer, err := nats.NewServer(s.opts.NatsConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err,
			"nats_config": s.opts.NatsConfig,
		}).Error("[NATS] inMemoryState: failed to configure NATS server")
		return err
	}

	err = messagingServer.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err,
			"nats_config": s.opts.NatsConfig,
		}).Error("[NATS] inMemoryState: failed to start NATS server")
		return err
	} else {
		log.WithFields(log.Fields{
			"host":         s.opts.NatsConfig.Host,
			"port":         s.opts.NatsConfig.Port,
			"http_port":    s.opts.NatsConfig.HTTPPort,
			"cluster_port": s.opts.NatsConfig.ClusterPort,
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
			"nats_config": s.opts.NatsConfig,
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

func probeURL(url string) error {
	httpClient := http.DefaultClient
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"error":   err,
			"address": url,
		}).Error("[NATS] failed to prepare request for NATS connectivity check")
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := httpClient.Do(req)
	if err != nil {
		// log.WithFields(log.Fields{
		// 	"error":   err,
		// 	"address": url,
		// }).Debug("[NATS] ping failed")
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *InMemoryState) updateMessagingClusterConns() error {
	new := []string{}

	mu := &sync.Mutex{}
	// httpClient := http.DefaultClient

	for nodeID, addresses := range s.serverAddressesCache {
		if nodeID == s.NodeID() {
			// nothing to do
			continue
		}

		wg := &sync.WaitGroup{}
		wg.Add(len(addresses))
		for _, a := range addresses {
			httpURL := fmt.Sprintf("http://%s:%d", a, nats.DefaultHTTPPort)
			clusterURL := fmt.Sprintf("nats://%s:%d", a, nats.DefaultClusterPort)
			go func(url, cluster string) {
				defer wg.Done()
				err := probeURL(url)
				if err != nil {
					// nothing to do
					return
				}
				mu.Lock()
				new = append(new, cluster)
				mu.Unlock()
			}(httpURL, clusterURL)
		}

		wg.Wait()
	}

	current := strings.Split(s.opts.NatsConfig.RoutesStr, ",")
	sort.Strings(new)
	sort.Strings(current)

	if len(new) > 0 && !reflect.DeepEqual(new, current) {
		log.WithFields(log.Fields{
			"new":     new,
			"current": current,
		}).Info("[inMemoryState.updateMessagingClusterConns] NATS routes changed, updating...")

		s.opts.NatsConfig.RoutesStr = strings.Join(new, ",")
		s.messagingServer.Shutdown()

		messagingServer, err := nats.NewServer(s.opts.NatsConfig)
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"nats_config": s.opts.NatsConfig,
			}).Error("inMemoryState: failed to configure NATS server")
			return err
		}

		err = messagingServer.Start()
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"nats_config": s.opts.NatsConfig,
			}).Error("inMemoryState: failed to start NATS server")
			return err
		}

		s.messagingServer = messagingServer
	}

	return nil
}
