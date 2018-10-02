package nats

import (
	"context"
	"time"

	// "github.com/dotmesh-io/dotmesh/pkg/stopper"
	// "github.com/dotmesh-io/dotmesh/pkg/timeutil"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/notification"
	"github.com/dotmesh-io/dotmesh/pkg/types"

	nats "github.com/nats-io/go-nats"

	log "github.com/sirupsen/logrus"
)

const (
	EnvNatsURL      = "NATS_URL"
	EnvNatsUsername = "NATS_USERNAME"
	EnvNatsPassword = "NATS_PASSWORD"

	EnvNatsSubjectPrefix = "NATS_SUBJECT_PREFIX"
)

type publisher struct {
	client        *nats.Conn
	encodedClient *nats.EncodedConn
	subject       string
	initialized   bool
}

type config struct {
	url      string
	username string
	password string
	prefix   string
}

func init() {
	notification.RegisterPublisher("nats", &publisher{})
}

func (p *publisher) Configure(c *notification.Config) (bool, error) {

	nastConfig := &config{}

	if os.Getenv(EnvNatsURL) != "" {
		nastConfig.url = os.Getenv(EnvNatsURL)
	} else {
		return false, nil
	}

	if os.Getenv(EnvNatsUsername) != "" {
		nastConfig.username = os.Getenv(EnvNatsUsername)
	} else {
		log.Warnf("%s env variable not supplied for NATS publisher", EnvNatsUsername)
	}
	if os.Getenv(EnvNatsPassword) != "" {
		nastConfig.password = os.Getenv(EnvNatsPassword)
	} else {
		log.Warnf("%s env variable not supplied for NATS publisher", EnvNatsPassword)
	}

	if os.Getenv(EnvNatsSubjectPrefix) != "" {
		nastConfig.prefix = os.Getenv(EnvNatsSubjectPrefix)
	} else {
		log.Warnf("%s env variable not supplied for NATS publisher", EnvNatsSubjectPrefix)
	}

	client, err := p.connect(nastConfig)
	if err != nil {
		return false, err
	}

	p.client = client

	encodedClient, err := nats.NewEncodedConn(client, nats.JSON_ENCODER)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to create encoded connection for NATS publisher")
		return false, err
	}

	p.encodedClient = encodedClient

	if nastConfig.prefix != "" {
		p.subject = nastConfig.prefix + "." + types.NATSPublishCommitsSubject
	} else {
		p.subject = types.NATSPublishCommitsSubject
	}

	p.initialized = true

	return true, nil
}

func (p *publisher) connect(cfg *config) (*nats.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var client *nats.Conn
	var err error

	for {
		select {
		case <-ctx.Done():
			return client, err
		default:
			if cfg.username != "" {
				client, err = nats.Connect(cfg.url, nats.UserInfo(cfg.username, cfg.password))
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}
			} else {
				client, err = nats.Connect(cfg.url)
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}
			}

			return client, err
		}
	}
}

// PublishCommit - publish commit to NATS
func (p *publisher) PublishCommit(event *types.CommitNotification) error {
	if p.initialized {
		return p.encodedClient.Publish(p.subject, event)
	}
	return nil
}
