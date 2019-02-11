package nats

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/gnatsd/server"

	log "github.com/sirupsen/logrus"
)

type NatsServer struct {
	server *server.Server
}

// NewServer creates a new gnatsd server from the given config and listens on
// the configured port.
func NewServer(config *Config) (*NatsServer, error) {

	opts := server.Options{}
	opts.Host = config.Host
	opts.Port = config.Port
	opts.HTTPHost = config.Host
	opts.HTTPPort = config.HTTPPort
	opts.RoutesStr = config.RoutesStr
	opts.Logtime = config.Logtime
	opts.Debug = config.Debug
	opts.Trace = config.Trace

	log.WithFields(log.Fields{
		"port":         opts.Port,
		"host":         opts.Host,
		"http_port":    opts.HTTPPort,
		"cluster_port": config.ClusterPort,
		"debug":        opts.Debug,
	}).Info("[NATS server] preparing server configuration")
	// Configure cluster opts if explicitly set via flags.
	err := configureClusterOpts(config.ClusterPort, &opts)
	if err != nil {
		return nil, fmt.Errorf("server configuration failed: %s", err)
	}

	s := server.New(&opts)
	if s == nil {
		return nil, fmt.Errorf("No NATS server object returned")
	}

	s.ConfigureLogger()

	logger := NewLogger()

	// Configure the logger based on the flags
	s.SetLogger(logger, opts.Debug, opts.Trace)

	return &NatsServer{
		server: s,
	}, nil

}

func (s *NatsServer) Start() error {
	// if s.server == nil {
	// 	return fmt.Errorf("server not initialized")
	// }

	go server.Run(s.server)
	if s.server.ReadyForConnections(10 * time.Second) {
		log.Info("[NATS server] ready for connections...")
		return nil
	}

	return fmt.Errorf("unable to start NATS server")
}

func (s *NatsServer) Shutdown() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func configureClusterOpts(clusterPort int, opts *server.Options) error {

	// If we have routes, fill in here.
	if opts.RoutesStr != "" {
		opts.Routes = server.RoutesFromStr(opts.RoutesStr)
	}

	opts.Cluster.Port = clusterPort
	opts.Cluster.Host = opts.Host

	return nil
}

// Logger implements the nats logging interface, backed with our own logrus logger
type Logger struct {
	logger *log.Entry
	debug  bool
}

// NewLogger creates a logger backed by logrus.  Logs everything at debug level,
// any user-facing messages should be written elsewhere.
func NewLogger() *Logger {
	enableNatsDebug := false
	if os.Getenv("NATS_DEBUG") == "true" {
		enableNatsDebug = true
	}
	return &Logger{
		logger: log.WithField("category", "nats"),
		debug:  enableNatsDebug,
	}
}

// Noticef logs a notice statement, which we log as debug
func (l *Logger) Noticef(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}

// Errorf logs an error statement, which we log as debug
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}

// Fatalf logs a fatal error, which we log as debug
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}

// Debugf logs a debug statement
func (l *Logger) Debugf(format string, v ...interface{}) {
	if !l.debug {
		// discarding nats debug logs
		return
	}
	l.logger.Debugf(format, v...)
}

// Tracef logs a trace statement, which we log as debug
func (l *Logger) Tracef(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}
