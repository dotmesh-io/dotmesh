package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"strings"
	// "log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/gorilla/mux"

	log "github.com/sirupsen/logrus"
)

type S3Handler struct {
	state *InMemoryState
	httputil.ReverseProxy
}

func NewS3Handler(state *InMemoryState) http.Handler {
	h := &S3Handler{
		state: state,
	}

	h.ReverseProxy.Director = h.Director

	return h
}

func (s *S3Handler) Director(req *http.Request) {
	target, ok := ctxGetAddress(req.Context())
	if !ok || target == "" {
		log.WithFields(log.Fields{
			"host": req.Host,
		}).Error("no target")

		_, cancel := context.WithCancel(req.Context())
		cancel()

		return
	}

	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}

	u, err := url.Parse(target)
	if err != nil {
		log.WithFields(log.Fields{
			"host":  req.Host,
			"error": err,
		}).Error("failed to parse URL")
		return
	}

	req.URL.Scheme = u.Scheme
	req.URL.Host = u.Host

	if _, ok := req.Header["User-Agent"]; !ok {
		// explicitly disable User-Agent so it's not set to default value
		req.Header.Set("User-Agent", "")
	}
}

type ctxKey string

const (
	ctxKeyTarget ctxKey = "target"
)

func ctxSetAddress(ctx context.Context, address string) context.Context {
	return context.WithValue(ctx, ctxKeyTarget, address)
}

func ctxGetAddress(ctx context.Context) (string, bool) {
	val := ctx.Value(ctxKeyTarget)
	if val == nil {
		return "", false
	}
	return val.(string), true
}

func (s *S3Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	volName := VolumeName{
		Name:      vars["name"],
		Namespace: vars["namespace"],
	}
	isAdmin, err := AuthenticatedUserIsNamespaceAdministrator(req.Context(), volName.Namespace)
	if err != nil {
		http.Error(resp, err.Error(), 401)
		return
	}
	if !isAdmin {
		http.Error(resp, "User is not the administrator of namespace "+volName.Namespace, 401)
		return
	}
	branch, ok := vars["branch"]
	bucketName := fmt.Sprintf("%s-%s", vars["namespace"], vars["name"])
	if !ok || branch == "master" {
		branch = ""
	} else {
		bucketName += "-" + branch
	}
	localFilesystemId := s.state.registry.Exists(
		volName, branch,
	)
	if localFilesystemId != "" {
		key, ok := vars["key"]
		if ok {
			switch req.Method {
			case "PUT":

				master := s.state.masterFor(localFilesystemId)
				if master != s.state.myNodeId {
					admin, err := s.state.userManager.Get(&user.Query{Ref: "admin"})
					if err != nil {
						http.Error(resp, fmt.Sprintf("Can't get API key to proxy s3 request: %+v.\n", err), 500)
						log.Errorf("can't get API key to proxy s3: %+v.", err)
						return
					}

					addresses := s.state.addressesFor(master)
					target, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey) // FIXME, need master->name mapping, see how handover works normally
					if err != nil {
						http.Error(resp, err.Error(), 500)
						log.Errorf("can't establish URL to proxy s3: %+v.", localFilesystemId, err)
						return
					}

					s.ServeHTTP(resp, req.WithContext(ctxSetAddress(req.Context(), target)))
					return
				}

				s.putObject(resp, req, localFilesystemId, key)
			}

		} else {
			switch req.Method {
			case "GET":
				s.listBucket(resp, req, bucketName, localFilesystemId)
			}
		}

	} else {
		resp.WriteHeader(404)
		resp.Write([]byte(fmt.Sprintf("Bucket %s does not exist", bucketName)))
	}
}

func (s *S3Handler) proxyConn(resp http.ResponseWriter, req *http.Request) {

}

func (s *S3Handler) putObject(resp http.ResponseWriter, req *http.Request, filesystemId, filename string) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		// todo better erroring
		resp.WriteHeader(400)
		return
	}

	// TODO(karolis): delete, need to write directly to the filesystem
	user := auth.GetUserFromCtx(req.Context())
	responseChan, _, err := s.state.globalFsRequestId(
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

	// s.state.filesystemsLock.Lock()
	// fsMachine, ok := s.state.filesystems[filesystemId]
	// if !ok {		
		fsMachine = s.state.initFilesystemMachine(filesystemId)
	// }
	// s.state.filesystemsLock.Unlock()

	fsMachine.innerRequests <- 

	if err != nil {
		// todo better erroring
		resp.WriteHeader(400)
		return
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

func (s *S3Handler) listBucket(resp http.ResponseWriter, req *http.Request, name string, filesystemId string) {
	snapshots, err := s.state.snapshotsForCurrentMaster(filesystemId)
	if len(snapshots) == 0 {
		// throw up an error?
		log.Println("no snaps")
		resp.WriteHeader(400)
		resp.Write([]byte("No snaps to mount - commit before listing."))

		return
	}
	lastSnapshot := snapshots[len(snapshots)-1]
	responseChan, err := s.state.globalFsRequest(
		filesystemId,
		&Event{Name: "mount-snapshot",
			Args: &EventArgs{"snapId": lastSnapshot.Id}},
	)
	if err != nil {
		// error here
		log.Println(err)
		return
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
