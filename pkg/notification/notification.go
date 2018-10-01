package notification

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/stopper"
	"github.com/dotmesh-io/dotmesh/pkg/timeutil"
	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

const (
	notifierMaxBackOff = 15 * time.Minute

	logPublisherName = "publisher name"
	logNotiName      = "notification name"
)

var (
	publishersM sync.RWMutex
	publishers  = make(map[string]Publisher)
)

// Config is the configuration for the publicher service and its registered
// publishers.
type Config struct {
	Attempts int
	Params   map[string]interface{} `yaml:",inline"`
}

// Publisher represents anything that can transmit notifications.
type Publisher interface {
	// Configure attempts to initialize the notifier with the provided configuration.
	// It returns whether the notifier is enabled or not.
	Configure(*Config) (bool, error)

	// PublishCommit informs the existence of the specified notification.
	PublishCommit(event *types.CommitNotification) error
}

// RegisterPublisher makes a Sender available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Sender is nil, this function panics.
func RegisterPublisher(name string, p Publisher) {
	if name == "" {
		panic("notification: could not register a publisher with an empty name")
	}

	if p == nil {
		panic("notification: could not register a nil Publisher")
	}

	publishersM.Lock()
	defer publishersM.Unlock()

	if _, dup := publishers[name]; dup {
		panic("notification: RegisterPublisher called twice for " + name)
	}

	log.WithFields(log.Fields{
		"name": name,
	}).Debug("extension.notification: publisher registered")

	publishers[name] = p
}

// DefaultNotificationPublisher - default notification publisher, manages configuration
type DefaultNotificationPublisher struct {
	config  *Config
	stopper *stopper.Stopper
}

// New - create new publisher
func New(ctx context.Context) *DefaultNotificationPublisher {
	return &DefaultNotificationPublisher{
		stopper: stopper.NewStopper(ctx),
	}
}

// Configure - configure is used to register multiple notification publishers
func (p *DefaultNotificationPublisher) Configure(config *Config) (bool, error) {
	p.config = config
	// Configure registered notifiers.
	for publisherName, publisher := range p.Publishers() {
		if configured, err := publisher.Configure(config); configured {
			log.WithField(logPublisherName, publisherName).Info("notificationSender: publisher configured")
		} else {
			p.UnregisterPublisher(publisherName)
			if err != nil {
				log.WithError(err).WithField(logPublisherName, publisherName).Error("could not configure notifier")
			}
		}
	}

	return true, nil
}

// Publishers returns the list of the registered Publishers.
func (p *DefaultNotificationPublisher) Publishers() map[string]Publisher {
	publishersM.RLock()
	defer publishersM.RUnlock()

	ret := make(map[string]Publisher)
	for k, v := range publishers {
		ret[k] = v
	}

	return ret
}

// Send - send notifications through all configured publishers
func (p *DefaultNotificationPublisher) PublishCommit(event *types.CommitNotification) error {

	publishersM.RLock()
	defer publishersM.RUnlock()

	for publisherName, publisher := range p.Publishers() {
		// TODO: move this into goroutine if we have enough publishers
		var attempts int
		var backOff time.Duration
		for {
			// Max attempts exceeded.
			if attempts >= p.config.Attempts {
				log.WithFields(log.Fields{
					logNotiName:      event.Name,
					logPublisherName: publisherName,
					"max attempts":   p.config.Attempts,
				}).Info("giving up on publishing notification : max attempts exceeded")
				return fmt.Errorf("failed to publish notification, max attempts (%d) reached", p.config.Attempts)
			}

			// Backoff
			if backOff > 0 {
				log.WithFields(log.Fields{
					"duration":       backOff,
					logNotiName:      event.Name,
					logPublisherName: publisherName,
					"attempts":       attempts + 1,
					"max attempts":   p.config.Attempts,
				}).Info("waiting before retrying to publish notification")
				if !p.stopper.Sleep(backOff) {
					return nil
				}
			}

			if err := publisher.PublishCommit(event); err != nil {
				// Send failed; increase attempts/backoff and retry.
				log.WithError(err).WithFields(log.Fields{logPublisherName: publisherName, logNotiName: event.Name}).Error("could not publish notification via notifier")
				backOff = timeutil.ExpBackoff(backOff, notifierMaxBackOff)
				attempts++
				continue
			}

			// Publish has been successful. Go to the next notifier.
			break
		}
	}

	return nil
}

// UnregisterPublisher removes a publisher with a particular name from the list.
func (p *DefaultNotificationPublisher) UnregisterPublisher(name string) {
	publishersM.Lock()
	defer publishersM.Unlock()

	delete(publishers, name)
}
