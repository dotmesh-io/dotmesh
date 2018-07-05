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
	Port     int
}

func NewJsonRpcClient(user, hostname, apiKey string, port int) *JsonRpcClient {
	return &JsonRpcClient{
		User:     user,
		Hostname: hostname,
		ApiKey:   apiKey,
		Port:     port,
	}
}

// TODO remove duplication wrt dm/pkg/api/remotes.go
// call a method with args, and attempt to decode it into result
func (j *JsonRpcClient) CallRemote(
	ctx context.Context, method string, args interface{}, result interface{},
) error {
	// RPCs are always between clusters, so "external"
	var url string
	var err error
	if j.Port == 0 {
		url, err = deduceUrl(ctx, []string{j.Hostname}, "external", j.User, j.ApiKey)
		if err != nil {
			return err
		}
	} else {
		url = fmt.Sprintf("http://%s:%d", j.Hostname, j.Port)
	}
	url = fmt.Sprintf("%s/rpc", url)
	return j.reallyCallRemote(ctx, method, args, result, url)
}

func (j *JsonRpcClient) reallyCallRemote(
	ctx context.Context, method string, args interface{}, result interface{},
	urlToUse string,
) error {
	// create new span using span found in context as parent (if none is found,
	// our span becomes the trace root).
	span, ctx := opentracing.StartSpanFromContext(ctx, method)

	span.SetTag("type", "dotmesh-server rpc")
	span.SetTag("rpcMethod", method)
	span.SetTag("rpcArgs", fmt.Sprintf("%v", args))
	defer span.Finish()

	url := urlToUse
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
		span.SetTag("error", fmt.Sprintf("Response '%s' yields error %s", string(b), err))
		return fmt.Errorf("Response '%s' yields error %s", string(b), err)
	}
	return nil
}
