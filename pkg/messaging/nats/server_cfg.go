package nats

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
)

const (
	// DefaultPort is the default port for client connections.
	// DefaultPort = nats.DefaultPort
	DefaultPort = 32609

	// DefaultHost defaults to all interfaces.
	DefaultHost = "0.0.0.0"

	// DefaultClusterPort is the default cluster communication port.
	DefaultClusterPort = 32610 // port for inbound route connections

	DefaultHTTPPort = 32611 // HTTP monitoring port
)

// Config holds the configuration for the messaging client and server.
type Config struct {
	Host        string
	Port        int
	ClusterPort int
	HTTPPort    int
	RoutesStr   string
	Logtime     bool
	Debug       bool
	Trace       bool
}

// DefaultConfig returns the default options for the messaging client & server.
func DefaultConfig() *Config {
	return &Config{
		Host:        DefaultHost,
		Port:        DefaultPort,
		ClusterPort: DefaultClusterPort,
		HTTPPort:    DefaultHTTPPort,
		Logtime:     true,
		Debug:       true,
		Trace:       true,
	}
}

// SetURL parses a string in the form nats://server:port and saves as options.
func (c *Config) SetURL(s string) error {

	u, err := url.Parse(s)
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return err
	}
	c.Host = host

	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	c.Port = p

	return nil
}

// GetURL returns a nats URL from a host & port.
func GetURL(host string, port int) string {

	if host == "" || port == 0 {
		return ""
	}
	return fmt.Sprintf("nats://%s:%d", host, port)
}
