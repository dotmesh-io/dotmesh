package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/net/context"

	"github.com/gorilla/rpc/v2/json2"
	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing/examples/middleware"
)

// RPC client for inter-cluster operation

type JsonRpcClient struct {
	User     string
	Hostname string
	ApiKey   string
}

func NewJsonRpcClient(user, hostname, apiKey string) *JsonRpcClient {
	return &JsonRpcClient{
		User:     user,
		Hostname: hostname,
		ApiKey:   apiKey,
	}
}

// TODO remove duplication wrt dm/pkg/api/remotes.go
// call a method with string args, and attempt to decode it into result
func (j *JsonRpcClient) CallRemote(
	ctx context.Context, method string, args interface{}, result interface{},
) error {
	// create new span using span found in context as parent (if none is found,
	// our span becomes the trace root).
	span, ctx := opentracing.StartSpanFromContext(ctx, method)
	span.SetTag("type", "dotmesh-server rpc")
	span.SetTag("rpcMethod", method)
	span.SetTag("rpcArgs", fmt.Sprintf("%v", args))
	defer span.Finish()

	// RPCs are always between clusters, so "external"
	url := fmt.Sprintf("%s/rpc", deduceUrl(j.Hostname, "external"))
	message, err := json2.EncodeClientRequest(method, args)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(message))
	if err != nil {
		return err
	}

	tracer := opentracing.GlobalTracer()
	// use our middleware to propagate our trace
	req = middleware.ToHTTPRequest(tracer)(req.WithContext(ctx))

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(j.User, j.ApiKey)
	client := new(http.Client)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		// TODO add user mgmt subcommands, then reference them in this error message
		// annotate our span with the error condition
		span.SetTag("error", "Permission denied")
		return fmt.Errorf("Permission denied. Please check that your API key is still valid.")
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.SetTag("error", err.Error())
		return fmt.Errorf("Error reading body: %s", err)
	}
	err = json2.DecodeClientResponse(bytes.NewBuffer(b), &result)
	if err != nil {
		span.SetTag("error", fmt.Sprintf("Couldn't decode response '%s': %s", string(b), err))
		return fmt.Errorf("Couldn't decode response '%s': %s", string(b), err)
	}
	return nil
}
