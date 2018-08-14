package main

import (
	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/gorilla/mux"
	"github.com/urfave/negroni"
)

type DotmeshS3 struct {
	usersManager user.UserManager
	state        *InMemoryState
}

func NewDotmeshS3(um user.UserManager) *DotmeshS3 {
	return &DotmeshS3{usersManager: um}
}

func (s *DotmeshS3) Serve() error {

	router := mux.NewRouter()

	s.registerRoutes(router)

	n := negroni.Classic()

	if os.Getenv("CORS") == "true" {
		n.Use(negroni.HandlerFunc(CorsHeadersMiddleware))
	}

	n.Use(negroni.NewRecovery())
	n.Use(negroni.HandlerFunc(s.InstrumentationMiddleware))
	n.Use(negroni.HandlerFunc(s.AuthenticationMiddleware))

	n.UseHandler(router)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.opts.Port),
		Handler:           n,
		IdleTimeout:       time.Second * 120,
		ReadTimeout:       time.Second * 30,
		ReadHeaderTimeout: time.Second * 30,
		WriteTimeout:      time.Second * 25,
	}
	s.server = server

	return server.ListenAndServe()
}
func (s *DotmeshS3) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *DotmeshS3) registerRoutes(mux *mux.Router) {
	mux.HandleFunc("/s3/{dotname}/{key}", s.s3PutHandler).Methods("PUT", "OPTIONS")
	mux.HandleFunc("/s3/{dotname}", s.s3ListHandler).Methods("LIST", "OPTIONS")
}

func (s *DotmeshS3) s3PutHandler(resp http.ResponseWriter, req *http.Request) {
	user := auth.GetUser(req)
	vars := mux.Vars(r)
	dotName := vars["dotname"]

	localFilesystemId := d.state.registry.Exists(
		dotName, "master",
	)
	if localFilesystemId != "" {
		log.Println("CREATE OBJECT:", dotName, vars["key"])
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
		responseChan, requestId, err := d.state.globalFsRequestId(
			localFilesystemId,
			&Event{Name: "put-file",
				Args: &EventArgs{
					"key":  vars["key"],
					"data": body,
				},
			},
		)
		if err != nil {
			return err
		}
		go func() {
			// asynchronously throw away the response, transfers can be polled via
			// their own entries in etcd
			e := <-responseChan
			log.Printf("finished saving %s, %+v", vars["key"], e)
		}()

		*result = requestId
		return nil
		resp.WriteHeader(200)
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.Write([]byte{})
	}

}
