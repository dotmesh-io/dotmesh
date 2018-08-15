package main

import (
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

type S3Handler struct {
	state *InMemoryState
}

func NewS3Handler(state *InMemoryState) http.Handler {
	return &S3Handler{
		state: state,
	}
}

func (s3 *S3Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	volName := VolumeName{
		Name:      vars["name"],
		Namespace: vars["namespace"],
	}
	localFilesystemId := s3.state.registry.Exists(
		volName, "master",
	)
	if localFilesystemId != "" {
		log.Println("CREATE OBJECT: %#v, %s", volName, vars["key"])
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			// todo better erroring
			resp.WriteHeader(400)
		}
		responseChan, _, err := s3.state.globalFsRequestId(
			localFilesystemId,
			&Event{Name: "put-file",
				Args: &EventArgs{
					"key":  vars["key"],
					"data": body,
				},
			},
		)
		if err != nil {
			// todo better erroring
			resp.WriteHeader(400)
		}
		go func() {
			// asynchronously throw away the response, transfers can be polled via
			// their own entries in etcd
			e := <-responseChan
			log.Printf("finished saving %s, %+v", vars["key"], e)
		}()
		resp.WriteHeader(200)
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.Write([]byte{})
	}

}
