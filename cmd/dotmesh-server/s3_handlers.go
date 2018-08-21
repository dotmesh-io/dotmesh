package main

import (
	"encoding/xml"
	"fmt"
	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"time"
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
		resp.Write([]byte(err.Error()))
	}
	if !isAdmin {
		resp.WriteHeader(401)
		resp.Write([]byte("User is not the administrator of namespace " + volName.Namespace))
	}
	branch, ok := vars["branch"]
	bucketName := fmt.Sprintf("%s-%s", vars["namespace"], vars["name"])
	if !ok || branch == "master" {
		branch = ""
	} else {
		bucketName += "-" + branch
	}
	localFilesystemId := s3.state.registry.Exists(
		volName, branch,
	)
	if localFilesystemId != "" {
		key, ok := vars["key"]
		if ok {
			switch req.Method {
			case "PUT":
				s3.putObject(resp, req, localFilesystemId, key)
			}

		} else {
			switch req.Method {
			case "GET":
				s3.listBucket(resp, req, bucketName, localFilesystemId)
			}
		}

	} else {
		resp.WriteHeader(404)
		resp.Write([]byte(fmt.Sprintf("Bucket %s does not exist", bucketName)))
	}

}

func (s3 *S3Handler) putObject(resp http.ResponseWriter, req *http.Request, filesystemId, filename string) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		// todo better erroring
		resp.WriteHeader(400)
	}
	user, err := auth.GetUserFromCtx(req.Context())
	if err != nil {
		resp.WriteHeader(400)
	}
	responseChan, _, err := s3.state.globalFsRequestId(
		filesystemId,
		&Event{Name: "put-file",
			Args: &EventArgs{
				"S3Request": S3ApiRequest{
					Filename:    filename,
					Data:        body,
					RequestType: "PUT",
					User:        user.Name,
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
		log.Printf("finished saving %s, %+v", filename, e)
	}()
	resp.WriteHeader(200)
	resp.Header().Set("Access-Control-Allow-Origin", "*")
	resp.Write([]byte{})
}

type ListBucketResult struct {
	Name     string
	Prefix   string
	Contents []BucketObject
}

type BucketObject struct {
	Key          string
	LastModified time.Time
	Size         int64
}

func (s3 *S3Handler) listBucket(resp http.ResponseWriter, req *http.Request, name string, filesystemId string) {
	snapshots, err := s3.state.snapshotsForCurrentMaster(filesystemId)
	if len(snapshots) == 0 {
		// throw up an error?
		log.Println("no snaps")
		resp.Write([]byte("No snaps to mount - commit before listing."))
		resp.WriteHeader(400)
		return
	}
	lastSnapshot := snapshots[len(snapshots)-1]
	responseChan, err := s3.state.globalFsRequest(
		filesystemId,
		&Event{Name: "mount-snapshot",
			Args: &EventArgs{"snapId": lastSnapshot.Id}},
	)
	if err != nil {
		// error here
		log.Println(err)
	}

	e := <-responseChan
	if e.Name == "mounted" {
		log.Printf("snapshot mounted %s for filesystem %s", lastSnapshot.Id, filesystemId)
		result := (*e.Args)["mount-path"].(string)
		keys, _, _ := getKeysForDir(result+"/__default__", "") // todo err handling
		bucket := ListBucketResult{
			Name:     name,
			Contents: []BucketObject{},
		}
		for key, info := range keys {
			object := BucketObject{
				Key:          key,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			}
			bucket.Contents = append(bucket.Contents, object)
		}
		response, _ := xml.Marshal(bucket) // todo err handling
		resp.WriteHeader(200)
		resp.Write(response)
	} else {
		// todo error here
		log.Println(e)
		resp.WriteHeader(500)
	}
}
