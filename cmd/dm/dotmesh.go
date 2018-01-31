package main

import (
	"fmt"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/commands"

	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
)

var clientVersion string

func main() {
	// Set up enough opentracing infrastructure that spans will be injected
	// into outgoing HTTP requests, even if we're not going to push spans into
	// zipkin ourselves
	collector := &zipkin.NopCollector{}
	tracer, err := zipkin.NewTracer(
		zipkin.NewRecorder(collector, false, "127.0.0.1:0", "dotmesh-cli"),
		zipkin.ClientServerSameSpan(true),
		zipkin.TraceID128Bit(true),
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	opentracing.InitGlobalTracer(tracer)

	// pass in the version number exposed via
	// -ldflags "-X main.clientVersion=<version>"
	commands.SetVersion(clientVersion)

	// Execute the command
	if err := commands.MainCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}
