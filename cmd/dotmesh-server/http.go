package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	rpc "github.com/gorilla/rpc/v2"
	rpcjson "github.com/gorilla/rpc/v2/json2"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing/examples/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/satori/go.uuid"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/metrics"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
)

const REQUEST_ID = "X-Request-Id"

type rpcTracking struct {
	rpcDuration map[uuid.UUID]time.Time
	mutex       *sync.Mutex
}

var rpcTracker = rpcTracking{rpcDuration: make(map[uuid.UUID]time.Time), mutex: &sync.Mutex{}}

// setting up and running our http server
// rpc and replication live in rpc.go and replication.go respectively

func (state *InMemoryState) runServer() {
	go func() {
		// for debugging:
		// http://stackoverflow.com/questions/19094099/how-to-dump-goroutine-stacktraces
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	r := rpc.NewServer()
	r.RegisterCodec(rpcjson.NewCodec(), "application/json")
	r.RegisterCodec(rpcjson.NewCodec(), "application/json;charset=UTF-8")
	r.RegisterInterceptFunc(rpcInterceptFunc)
	r.RegisterAfterFunc(rpcAfterFunc)
	d := NewDotmeshRPC(state, state.userManager)
	err := r.RegisterService(d, "") // deduces name from type name
	if err != nil {
		log.Printf("Error while registering services %s", err)
	}

	router := mux.NewRouter()

	// only use the zipkin middleware if we have a TRACE_ADDR
	if os.Getenv("TRACE_ADDR") != "" {
		tracer := opentracing.GlobalTracer()

		router.Handle("/rpc",
			middleware.FromHTTPRequest(tracer, "rpc")(Instrument(state)(NewAuthHandler(r, state.userManager))),
		)

		router.Handle(
			"/filesystems/{filesystem}/{fromSnap}/{toSnap}",
			middleware.FromHTTPRequest(tracer, "zfs-sender")(
				Instrument(state)(NewAuthHandler(state.NewZFSSendingServer(), state.userManager)),
			),
		).Methods("GET")

		router.Handle(
			"/filesystems/{filesystem}/{fromSnap}/{toSnap}",
			middleware.FromHTTPRequest(tracer, "zfs-receiver")(
				Instrument(state)(NewAuthHandler(state.NewZFSReceivingServer(), state.userManager)),
			),
		).Methods("POST")

		// list files in the latest snapshot
		router.Handle("/s3/{namespace}:{name}", middleware.FromHTTPRequest(tracer, "s3")(Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager)))).Methods("GET")
		// list files in a specific snapshot
		router.Handle("/s3/{namespace}:{name}/snapshot/{snapshotId}", middleware.FromHTTPRequest(tracer, "s3")(Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager)))).Methods("GET")
		// download a file from a specific snapshot
		router.Handle("/s3/{namespace}:{name}/snapshot/{snapshotId}/{key:.*}", middleware.FromHTTPRequest(tracer, "s3")(Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager)))).Methods("GET")

		// put file into master
		router.Handle("/s3/{namespace}:{name}/{key:.*}", middleware.FromHTTPRequest(tracer, "s3")(Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager)))).Methods("PUT")

		// put file into other branch
		router.Handle("/s3/{namespace}:{name}@{branch}/{key:.*}", middleware.FromHTTPRequest(tracer, "s3")(Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager)))).Methods("PUT")
	} else {
		router.Handle("/rpc", Instrument(state)(NewAuthHandler(r, state.userManager)))

		router.Handle(
			"/filesystems/{filesystem}/{fromSnap}/{toSnap}",
			Instrument(state)(NewAuthHandler(state.NewZFSSendingServer(), state.userManager)),
		).Methods("GET")

		router.Handle(
			"/filesystems/{filesystem}/{fromSnap}/{toSnap}",
			Instrument(state)(NewAuthHandler(state.NewZFSReceivingServer(), state.userManager)),
		).Methods("POST")

		// list files in the latest snapshot
		router.Handle("/s3/{namespace}:{name}", Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager))).Methods("GET")
		// list files in a specific snapshot
		router.Handle("/s3/{namespace}:{name}/snapshot/{snapshotId}", Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager))).Methods("GET")
		// download a file from a specific snapshot
		router.Handle("/s3/{namespace}:{name}/snapshot/{snapshotId}/{key:.*}", Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager))).Methods("GET")
		// put file into master
		router.Handle("/s3/{namespace}:{name}/{key:.*}", Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager))).Methods("PUT")
		// put file into other branch
		router.Handle("/s3/{namespace}:{name}@{branch}/{key:.*}", Instrument(state)(NewAuthHandler(NewS3Handler(state), state.userManager))).Methods("PUT")
	}

	router.HandleFunc("/check",
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "OK")
		},
	)

	router.Handle("/metrics", promhttp.Handler())

	if os.Getenv("PRINT_HTTP_LOGS") != "" {
		loggingRouter := handlers.LoggingHandler(getLogfile("requests"), router)
		err = http.ListenAndServe(fmt.Sprintf(":%s", client.SERVER_PORT), loggingRouter)
	} else {
		err = http.ListenAndServe(fmt.Sprintf(":%s", client.SERVER_PORT), router)
	}

	if err != nil {
		utils.Out(fmt.Sprintf("Unable to listen on port %s: '%s'\n", client.SERVER_PORT, err))
		log.Fatalf("Unable to listen on port %s: '%s'", client.SERVER_PORT, err)
	}
}

func (state *InMemoryState) runUnixDomainServer() {
	// if we have disabled flexvolume then we are not running inside Kubernetes
	// and do not need the unix domain socket
	if os.Getenv("DISABLE_FLEXVOLUME") != "" {
		return
	}
	r := rpc.NewServer()
	r.RegisterCodec(rpcjson.NewCodec(), "application/json")
	r.RegisterCodec(rpcjson.NewCodec(), "application/json;charset=UTF-8")
	d := NewDotmeshRPC(state, state.userManager)
	err := r.RegisterService(d, "") // deduces name from type name
	if err != nil {
		log.Printf("[runUnixDomainServer] Error while registering services %s", err)
	}

	// UNIX socket for flexvolume driver to talk to us
	FV_SOCKET := FLEXVOLUME_DIR + "/dm.sock"

	// Unlink any old socket lingering there
	if _, err := os.Stat(FV_SOCKET); err == nil {
		if err = os.Remove(FV_SOCKET); err != nil {
			log.Fatalf("[runUnixDomainServer] Could not clean up existing socket at %s: %v", FV_SOCKET, err)
		}
	}

	listener, err := net.Listen("unix", FV_SOCKET)
	if err != nil {
		log.Fatalf("[runUnixDomainServer] Could not listen on %s: %v", FV_SOCKET, err)
	}

	unixSocketRouter := mux.NewRouter()
	unixSocketRouter.Handle("/rpc", r)

	// pre-authenticated-as-admin rpc server for clever unix socket clients
	// only. intended for use by the flexvolume driver, hence the location on
	// disk.
	http.Serve(listener, NewAdminHandler(unixSocketRouter))
}

// handler which makes all requests appear as the admin user!
// DANGER - only use for unix domain sockets.
func NewAdminHandler(handler http.Handler) http.Handler {
	return AdminHandler{subHandler: handler}
}

type AdminHandler struct {
	subHandler http.Handler
}

func (a AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(AdminContext(r.Context()))
	a.subHandler.ServeHTTP(w, r)
}

type MetricsMiddleware func(http.Handler) http.Handler

type instrResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func NewInstrResponseWriter(w http.ResponseWriter) *instrResponseWriter {
	return &instrResponseWriter{w, http.StatusOK}
}

func (irw *instrResponseWriter) WriteHeader(code int) {
	irw.statusCode = code
	irw.ResponseWriter.WriteHeader(code)
}

func Instrument(state *InMemoryState) MetricsMiddleware {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			irw := NewInstrResponseWriter(w)
			defer func() {
				duration := time.Since(startedAt)
				statusCode := fmt.Sprintf("%v", irw.statusCode)
				metrics.RequestDuration.WithLabelValues(r.URL.String(), r.Method, statusCode).Observe(duration.Seconds())
				metrics.RequestCounter.WithLabelValues(r.URL.String(), r.Method, statusCode).Add(1)
			}()
			h.ServeHTTP(irw, r)
		})
	}
}

func rpcAfterFunc(reqInfo *rpc.RequestInfo) {
	reqId, ok := reqInfo.Request.Header[REQUEST_ID]
	if ok && len(reqId) != 0 {
		reqUUID, err := uuid.FromString(reqId[0])
		if err != nil {
			fmt.Printf("Error: Unable to parse requestID UUID: %s", reqId)
			return
		}
		rpcTracker.mutex.Lock()
		defer rpcTracker.mutex.Unlock()
		startedAt, found := rpcTracker.rpcDuration[reqUUID]
		if !found {
			fmt.Printf("Error: Unable to find requestUUID in requestTracker: %s", reqUUID)
			return
		}
		duration := time.Since(startedAt)
		statusCode := fmt.Sprintf("%v", reqInfo.StatusCode)
		metrics.RPCRequestDuration.WithLabelValues(reqInfo.Request.URL.String(), reqInfo.Method, statusCode).Observe(duration.Seconds())
		delete(rpcTracker.rpcDuration, reqUUID)
	}
}

func rpcInterceptFunc(reqInfo *rpc.RequestInfo) *http.Request {
	reqId := uuid.NewV4()
	reqInfo.Request.Header.Set(REQUEST_ID, reqId.String())
	rpcTracker.mutex.Lock()
	defer rpcTracker.mutex.Unlock()
	rpcTracker.rpcDuration[reqId] = time.Now()
	return reqInfo.Request
}
