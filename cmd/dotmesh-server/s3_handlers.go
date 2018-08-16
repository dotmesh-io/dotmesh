package main

import (
	"fmt"
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
	isAdmin, err := AuthenticatedUserIsNamespaceAdministrator(req.Context(), volName.Namespace)
	if err != nil {
		resp.WriteHeader(401)
		resp.Write([]byte("User is not the administrator of namespace "+volName.Namespace))
	}
	branch, ok := vars["branch"]
	bucketName := fmt.Sprintf("%s-%s", vars["name"], vars["namespace"])
	if !ok || branch == "master" {
		branch = ""
	} else {
		bucketName += "-" + branch
	}
	localFilesystemId := s3.state.registry.Exists(
		volName, branch,
	)
	if localFilesystemId != "" {
		log.Println("CREATE OBJECT:", volName, vars["key"])
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			// todo better erroring
			resp.WriteHeader(400)
		}
		responseChan, _, err := s3.state.globalFsRequestId(
			localFilesystemId,
			&Event{Name: "put-file",
				Args: &EventArgs{
					"S3Request": S3ApiRequest{
						Filename:    vars["key"],
						Data:        body,
						RequestType: "PUT",
					},
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
	} else {
		resp.WriteHeader(404)
		resp.Write([]byte(fmt.Sprintf("Bucket %s does not exist", bucketName)))
	}

}
