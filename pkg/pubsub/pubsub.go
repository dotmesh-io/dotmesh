package pubsub

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"sync"

	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/scrypt"
)

const (
	// Scrypt parameters, these are considered good as of 2017 according to https://godoc.org/golang.org/x/crypto/scrypt
	scryptN = 32768
	scryptR = 8
	scryptP = 1
	hashLen = 32
)

type CommitNotification struct {
	FilesystemId string
	Namespace    string
	Name         string
	Branch       string
	CommitId     string
	Metadata     map[string]string
}

type subscriptionConnection struct {
	pwHash        []byte
	client        *nats.Conn
	encodedClient *nats.EncodedConn
	refCount      int
}

type subscriptionTarget struct {
	url      string
	username string
	conn     *subscriptionConnection
	subject  string
}

type PubSubManager struct {
	salt  []byte
	mutex sync.Mutex
	cache map[string]*subscriptionConnection

	commitSubs []subscriptionTarget
}

func subsConKey(url, username string) string {
	return fmt.Sprintf("%s@%s", username, url)
}

func (cc *PubSubManager) hashPassword(password string) ([]byte, error) {
	return scrypt.Key([]byte(password), cc.salt, scryptN, scryptR, scryptP, hashLen)
}

func NewPubSubManager() *PubSubManager {
	salt := make([]byte, hashLen)
	_, err := rand.Read(salt)
	if err != nil {
		panic(err)
	}

	return &PubSubManager{
		salt:  salt,
		mutex: sync.Mutex{},
		cache: map[string]*subscriptionConnection{},
	}
}

func (cc *PubSubManager) claimConnection(url, username, password string) (*subscriptionConnection, error) {
	// Mutex must be held before calling this!
	conKey := subsConKey(url, username)
	con, ok := cc.cache[conKey]
	if ok {
		// Check the password is correct
		h, err := cc.hashPassword(password)
		if err != nil {
			return nil, err
		}

		if subtle.ConstantTimeCompare(h, con.pwHash) != 1 {
			log.Printf("[pubsub.claimConnection] Rejecting re-use of pubsub connection for %s @ %s", username, url)
			return nil, fmt.Errorf("Password mismatch for %s @ %s", username, url)
		}
		log.Printf("[pubsub.claimConnection] Re-using connection %#v for %s @ %s", con, username, url)
		// All good, increment refcount and return success
		con.refCount++
		return con, nil
	} else {
		// Need to create a new connection

		hash, err := cc.hashPassword(password)

		if err != nil {
			return nil, err
		}

		var lastErr error
		for retries := 0; retries < 10; retries++ {
			client, err := nats.Connect(url, nats.UserInfo(username, password))
			if err != nil {
				log.Printf("[pubsub.claimConnection] Error %#v connecting to %s @ %s...", err, username, url)
				lastErr = err
				continue // retry
			}

			encodedClient, err := nats.NewEncodedConn(client, nats.GOB_ENCODER)
			if err != nil {
				log.Printf("[pubsub.claimConnection] Error %#v connecting to %s @ %s...", err, username, url)
				lastErr = err
				continue // retry
			}

			sc := subscriptionConnection{
				pwHash:        hash,
				client:        client,
				encodedClient: encodedClient,
				refCount:      1,
			}
			cc.cache[conKey] = &sc
			log.Printf("[pubsub.claimConnection] Created connection %#v to %s @ %s...", sc, username, url)
			return &sc, nil
		}
		log.Printf("[pubsub.claimConnection] No more retries after error %#v connecting to %s @ %s...", lastErr, username, url)
		return nil, lastErr
	}
}

func (cc *PubSubManager) releaseConnection(url, username string) error {
	// Mutex must be held before calling this!
	conKey := subsConKey(url, username)
	con, ok := cc.cache[conKey]
	if ok {
		con.refCount--
		if con.refCount == 0 {
			con.encodedClient.Close()
			con.client.Close()
			delete(cc.cache, conKey)
		}
		return nil
	} else {
		return fmt.Errorf("Attempted release of a connection that is not currently claimed: %s @ %s", username, url)
	}
}

func (cc *PubSubManager) SubscribeForCommits(url, username, password, subject string) error {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	conn, err := cc.claimConnection(url, username, password)
	if err != nil {
		return err
	}

	cc.commitSubs = append(cc.commitSubs, subscriptionTarget{
		url:      url,
		username: username,
		conn:     conn,
		subject:  subject,
	})

	return nil
}

func (cc *PubSubManager) UnsubscribeForCommits(url, username, subject string) error {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	var err error
	err = nil

	for idx, cs := range cc.commitSubs {
		if cs.url == url && cs.username == username && cs.subject == subject {
			err = cc.releaseConnection(cs.url, cs.username)

			cc.commitSubs[idx] = cc.commitSubs[len(cc.commitSubs)-1] // Replace it with the last one.
			cc.commitSubs = cc.commitSubs[:len(cc.commitSubs)-1]     // Chop off the last one.
			return err
		}
	}

	return fmt.Errorf("Could not find subscription for commits for %s @ %s : %s", username, url, subject)
}

func (cc *PubSubManager) PublishCommit(fsId, namespace, name, branch, commitId string, metadata map[string]string) error {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cn := &CommitNotification{
		FilesystemId: fsId,
		Namespace:    namespace,
		Name:         name,
		Branch:       branch,
		CommitId:     commitId,
		Metadata:     metadata,
	}

	for _, cs := range cc.commitSubs {
		log.Printf("[PublishCommit] Sending %#v to %#v", cn, cs)
		err := cs.conn.encodedClient.Publish(cs.subject, cn)
		if err != nil {
			// Log the error, but don't stop trying other subscriptions
			log.Printf("[PublishCommit] WARNING: Failed to publish to %s @ %s : %s: %#v",
				cs.username, cs.url, cs.subject, err)
		}
	}

	return nil
}
