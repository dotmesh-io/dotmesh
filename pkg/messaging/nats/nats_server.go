package nats

import (
	"fmt"
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

	// Configure cluster opts if explicitly set via flags.
	err := configureClusterOpts(config.ClusterPort, &opts)
	if err != nil {
		server.PrintAndDie(err.Error())
	}

	s := server.New(&opts)
	if s == nil {
		return nil, fmt.Errorf("No NATS server object returned")
	}

	// Configure the logger based on the flags
	s.SetLogger(NewLogger(), opts.Debug, opts.Trace)

	return &NatsServer{
		server: s,
	}, nil

}

func (s *NatsServer) Start() error {
	if s.server == nil {
		return fmt.Errorf("server not initialized")
	}
	// Run server in Go routine.
	go s.server.Start()

	if s.server.ReadyForConnections(10 * time.Second) {
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
}

// NewLogger creates a logger backed by logrus.  Logs everything at debug level,
// any user-facing messages should be written elsewhere.
func NewLogger() *Logger {
	return &Logger{
		logger: log.WithField("category", "nats"),
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
	l.logger.Debugf(format, v...)
}

// Tracef logs a trace statement, which we log as debug
func (l *Logger) Tracef(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}
