package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/fsm"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/dotmesh-io/dotmesh/pkg/validator"
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
	if !validator.EnsureValidOrRespond(vars["namespace"], validator.IsValidVolumeNamespace, resp) {
		return
	}
	if !validator.EnsureValidOrRespond(vars["name"], validator.IsValidVolumeName, resp) {
		return
	}
	volName := VolumeName{
		Name:      vars["name"],
		Namespace: vars["namespace"],
	}

	isAdmin, err := AuthenticatedUserIsNamespaceAdministrator(req.Context(), volName.Namespace, s.state.opts.UserManager)
	if err != nil {
		log.Warn("[S3Handler.ServeHTTP] authentication failed")
		http.Error(resp, err.Error(), 401)
		return
	}
	if !isAdmin {
		errStr := fmt.Sprintf("User %s is not the administrator of namespace %s", auth.GetUserFromCtx(req.Context()).Name, volName.Namespace)
		log.Warn("[S3Handler.ServeHTTP] " + errStr)
		http.Error(resp, errStr, 401)
		return
	}
	branch, ok := vars["branch"]
	bucketName := fmt.Sprintf("%s-%s", vars["namespace"], vars["name"])
	if !ok || branch == "master" {
		branch = ""
	} else {
		if !validator.EnsureValidOrRespond(branch, validator.IsValidBranchName, resp) {
			return
		}
		bucketName += "-" + branch
	}
	localFilesystemId := s.state.registry.Exists(volName, branch)

	snapshotId, ok := vars["snapshotId"]
	if ok {
		if !validator.EnsureValidOrRespond(snapshotId, validator.IsValidSnapshotName, resp) {
			return
		}
	} else {
		snapshotId = ""
	}

	if localFilesystemId == "" {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("[S3Handler.ServeHTTP] filesystem not found")
		http.Error(resp, fmt.Sprintf("Bucket %s does not exist", bucketName), 404)
		return
	}

	l := log.WithFields(log.Fields{
		"method":     req.Method,
		"filesystem": localFilesystemId,
		"snapshot":   snapshotId,
		"bucket":     bucketName,
		"branch":     branch,
	})

	l.Info("[S3Handler.ServeHTTP] Request received")

	// ensure any of these requests end up on the current master node for
	// this filesystem
	master, err := s.state.registry.CurrentMasterNode(localFilesystemId)
	if err != nil {
		l.WithError(err).Error("[S3Handler.ServeHTTP] master node for filesystem not found")
		http.Error(resp, fmt.Sprintf("master node for filesystem %s not found", localFilesystemId), 500)
		return
	}
	if master != s.state.NodeID() {
		admin, err := s.state.userManager.Get(&user.Query{Ref: "admin"})
		if err != nil {
			http.Error(resp, fmt.Sprintf("Can't get API key to proxy s3 request: %+v.\n", err), 500)
			l.WithError(err).Error("[S3Handler.ServeHTTP] Can't get API key to proxy s3")
			return
		}
		addresses := s.state.AddressesForServer(master)
		target, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey) // FIXME, need master->name mapping, see how handover works normally
		if err != nil {
			http.Error(resp, err.Error(), 500)
			l.WithError(err).Error("[S3Handler.ServeHTTP] Can't establish URL to proxy")
			return
		}
		l.WithField("target", target).Info("[S3Handler.ServeHTTP] proxying PUT request")
		s.ReverseProxy.ServeHTTP(resp, req.WithContext(ctxSetAddress(req.Context(), target)))
		return
	}

	// from this point on we assume we are on the node that is the current master
	// for the filesystem
	key, ok := vars["key"]
	if ok {
		switch req.Method {
		case "HEAD":
			s.headFile(l, resp, req, localFilesystemId, snapshotId, key)
		case "GET":
			s.readFile(l, resp, req, localFilesystemId, snapshotId, key)
		case "PUT":
			s.putObject(l, resp, req, localFilesystemId, key)
		case "DELETE":
			s.deleteObject(l, resp, req, localFilesystemId, key)
		}
	} else {
		switch req.Method {
		case "GET":
			s.listBucket(l, resp, req, bucketName, localFilesystemId, snapshotId)
		}
	}
}

func (s *S3Handler) mountFilesystemSnapshot(filesystemId string, snapshotId string) *Event {
	snapshots, err := s.state.SnapshotsForCurrentMaster(filesystemId)
	if err != nil {
		return types.NewErrorEvent("snapshots-error", err)
	}
	if len(snapshots) == 0 {
		return types.NewEvent("no-snapshots-found")
	}
	lastSnapshot := snapshots[len(snapshots)-1]
	mountSnapshotId := lastSnapshot.Id
	if snapshotId != "" && snapshotId != "latest" {
		mountSnapshotId = snapshotId
	}
	responseChan, err := s.state.globalFsRequest(
		filesystemId,
		&Event{Name: "mount-snapshot",
			Args: &EventArgs{"snapId": mountSnapshotId}},
	)
	if err != nil {
		return types.NewErrorEvent("mount-snapshot-error", err)
	}

	e := <-responseChan
	return e
}

func (s *S3Handler) headFile(l *log.Entry, resp http.ResponseWriter, req *http.Request, filesystemId, snapshotId, filename string) {
	user := auth.GetUserFromCtx(req.Context())
	fsm, err := s.state.InitFilesystemMachine(filesystemId)
	if err != nil {
		http.Error(resp, "failed to initialize filesystem", http.StatusInternalServerError)
		l.WithError(err).Error("[S3Handler.headFile] failed to initialize filesystem")
		return
	}

	state := fsm.GetCurrentState()
	if state != "active" {
		http.Error(resp, fmt.Sprintf("please try again later, state was %s", state), http.StatusServiceUnavailable)
		l.WithField("state", state).Error("[S3Handler.headFile] FSM is not in active state")
		return
	}

	// we must first mount the given snapshot before we try to read a file within it
	// if snapshotId is not given then the latest snapshot id will be used
	e := s.mountFilesystemSnapshot(filesystemId, snapshotId)
	// the snapshot has been mounted - pass the SnapshotMountPath via the
	// OutputFile to the fileOutputIO channel to get handled
	if e.Name != "mounted" {
		log.Println(e)
		log.WithFields(log.Fields{
			"event":      e,
			"filesystem": filesystemId,
		}).Error("mount failed, returned event is not 'mounted'")
		http.Error(resp, fmt.Sprintf("failed to mount filesystem (%s), check logs", e.Name), 500)
		l.WithField("event", fmt.Sprintf("%#v", e)).Error("[S3Handler.headFile] failed to mount filesystem")
		return
	} else {
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")

		respCh := make(chan *Event)
		fsm.StatFile(&types.OutputFile{
			Filename:          filename,
			User:              user.Name,
			Response:          respCh,
			SnapshotMountPath: (*e.Args)["mount-path"].(string),
		})

		result := <-respCh

		switch result.Name {
		case types.EventNameReadFailed:
			err := result.Error()
			l.WithError(err).Error("[S3Handler.headFile] failed to read")
			if err != nil {
				http.Error(resp, err.Error(), 500)
				return
			}
			http.Error(resp, "read failed, could not retrieve actual error", 500)
		case types.EventNameFileNotFound:
			err := result.Error()
			l.WithError(err).Error("[S3Handler.headFile] failed to find file")
			if err != nil {
				http.Error(resp, err.Error(), 404)
				return
			}
			http.Error(resp, "file not found, could not retrieve actual error", 404)
		default:
			mode, ok := (*result.Args)["mode"]
			if ok && mode.(os.FileMode).IsRegular() {
				size, ok := (*result.Args)["size"]
				if ok {
					resp.Header().Set("Content-Length", fmt.Sprintf("%d", size.(int64)))
				}
			}
			resp.WriteHeader(200)
		}
	}
}

func (s *S3Handler) readFile(l *log.Entry, resp http.ResponseWriter, req *http.Request, filesystemId, snapshotId, filename string) {
	user := auth.GetUserFromCtx(req.Context())
	fsm, err := s.state.InitFilesystemMachine(filesystemId)
	if err != nil {
		http.Error(resp, "failed to initialize filesystem", http.StatusInternalServerError)
		l.WithError(err).Error("[S3Handler.readFile] failed to initialize filesystem")
		return
	}

	state := fsm.GetCurrentState()
	if state != "active" {
		http.Error(resp, fmt.Sprintf("please try again later, state was %s", state), http.StatusServiceUnavailable)
		l.WithField("state", state).Error("[S3Handler.readFile] FSM is not in active state")
		return
	}

	// we must first mount the given snapshot before we try to read a file within it
	// if snapshotId is not given then the latest snapshot id will be used
	e := s.mountFilesystemSnapshot(filesystemId, snapshotId)

	// the snapshot has been mounted - pass the SnapshotMountPath via the
	// OutputFile to the fileOutputIO channel to get handled
	if e.Name == "mounted" {
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")

		defer req.Body.Close()
		respCh := make(chan *Event)
		fsm.ReadFile(&types.OutputFile{
			Filename:          filename,
			Contents:          resp,
			User:              user.Name,
			Response:          respCh,
			SnapshotMountPath: (*e.Args)["mount-path"].(string),
		})

		result := <-respCh

		switch result.Name {
		case types.EventNameReadFailed:
			err := result.Error()
			l.WithError(err).Error("[S3Handler.readFile] failed to read")
			if err != nil {
				http.Error(resp, err.Error(), 500)
				return
			}
			http.Error(resp, "read failed, could not retrieve actual error", 500)
		case types.EventNameFileNotFound:
			err := result.Error()
			l.WithError(err).Error("[S3Handler.readFile] failed to find file")
			if err != nil {
				http.Error(resp, err.Error(), 404)
				return
			}
			http.Error(resp, "file not found, could not retrieve actual error", 404)
		default:
			resp.WriteHeader(200)
		}
	} else {
		log.Println(e)
		log.WithFields(log.Fields{
			"event":      e,
			"filesystem": filesystemId,
		}).Error("mount failed, returned event is not 'mounted'")
		http.Error(resp, fmt.Sprintf("failed to mount filesystem (%s), check logs", e.Name), 500)
		l.WithError(err).Error("[S3Handler.readFile] failed to mount filesystem")
	}
}

func (s *S3Handler) putObject(l *log.Entry, resp http.ResponseWriter, req *http.Request, filesystemId, filename string) {
	user := auth.GetUserFromCtx(req.Context())
	fsm, err := s.state.InitFilesystemMachine(filesystemId)
	if err != nil {
		http.Error(resp, "failed to initialize filesystem", http.StatusInternalServerError)
		l.WithError(err).Error("[S3Handler.putObject] failed to initialize filesystem")
		return
	}

	state := fsm.GetCurrentState()
	if state != "active" {
		http.Error(resp, fmt.Sprintf("please try again later, state was %s", state), http.StatusServiceUnavailable)
		l.WithField("state", state).Error("[S3Handler.putObject] FSM is not in active state")
		return
	}

	defer req.Body.Close()
	respCh := make(chan *Event)

	fsm.WriteFile(&types.InputFile{
		Filename: filename,
		Contents: req.Body,
		User:     user.Name,
		Response: respCh,
		Extract:  req.Header.Get("Extract") == "true",
	})

	result := <-respCh

	switch result.Name {
	case types.EventNameSaveFailed:
		e, ok := (*result.Args)["err"].(string)
		if ok {
			http.Error(resp, e, 500)
			l.WithField("error", e).Error("[S3Handler.putObject] put failed")
			return
		}
		l.Error("[S3Handler.putObject] put failed")
		http.Error(resp, "upload failed", 500)
	case types.EventNameSaveSuccess:
		log.WithFields(log.Fields{
			"filename":    filename,
			"user":        user.Name,
			"snapshot_id": result.Args.GetString("SnapshotId"),
		}).Info("file uploaded successfully")
		resp.Header().Set("Snapshot", result.Args.GetString("SnapshotId"))
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.WriteHeader(200)

	default:
		log.WithFields(log.Fields{
			"error":    "unexpected event type returned",
			"filename": filename,
			"user":     user.Name,
			"args":     result.Args,
		}).Error("unexpected event type after uploading file")
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.WriteHeader(200)
	}
}

func (s *S3Handler) deleteObject(l *log.Entry, resp http.ResponseWriter, req *http.Request, filesystemId, filename string) {
	user := auth.GetUserFromCtx(req.Context())
	fsm, err := s.state.InitFilesystemMachine(filesystemId)
	if err != nil {
		http.Error(resp, "failed to initialize filesystem", http.StatusInternalServerError)
		l.WithError(err).Error("[S3Handler.deleteObject] failed to initialize filesystem")
		return
	}

	state := fsm.GetCurrentState()
	if state != "active" {
		http.Error(resp, fmt.Sprintf("please try again later, state was %s", state), http.StatusServiceUnavailable)
		l.WithField("state", state).Error("[S3Handler.deleteObject] FSM is not in active state")
		return
	}

	defer req.Body.Close()
	respCh := make(chan *Event)

	fsm.WriteFile(&types.InputFile{
		Filename: filename,
		Contents: nil,
		User:     user.Name,
		Response: respCh,
	})

	result := <-respCh

	switch result.Name {
	case types.EventNameDeleteFailed:
		e, ok := (*result.Args)["err"].(string)
		if ok {
			http.Error(resp, e, 500)
			l.WithField("error", e).Error("[S3Handler.deleteObject] put failed")
			return
		}
		l.Error("[S3Handler.deleteObject] delete failed")
		http.Error(resp, "delete failed", 500)
	case types.EventNameDeleteSuccess:
		log.WithFields(log.Fields{
			"filename":    filename,
			"user":        user.Name,
			"snapshot_id": result.Args.GetString("SnapshotId"),
		}).Info("file deleted successfully")
		resp.Header().Set("Snapshot", result.Args.GetString("SnapshotId"))
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.WriteHeader(200)
	case types.EventNameFileNotFound:
		err := result.Error()
		l.WithError(err).Error("[S3Handler.deleteFile] failed to find file")
		if err != nil {
			http.Error(resp, err.Error(), 404)
			return
		}
		http.Error(resp, "file not found", 404)

	default:
		log.WithFields(log.Fields{
			"error":    "unexpected event type returned",
			"filename": filename,
			"user":     user.Name,
			"args":     result.Args,
		}).Error("unexpected event type after delete file")
		resp.Header().Set("Access-Control-Allow-Origin", "*")
		resp.WriteHeader(200)
	}
}

type ListBucketResult struct {
	Name         string               `json:"name"`
	Prefix       string               `json:"prefix"`
	Contents     []types.ListFileItem `json:"contents"`
	TotalResults int64                `json:"total_results"`
}

func (s *S3Handler) listBucket(l *log.Entry, resp http.ResponseWriter, req *http.Request, name string, filesystemId string, snapshotId string) {

	start := time.Now()
	e := s.mountFilesystemSnapshot(filesystemId, snapshotId)
	if time.Since(start) > 2*time.Second {
		l.WithFields(log.Fields{
			"duration": time.Since(start),
		}).Warn("s3Handler.listBucket: listing bucket contents is getting slow")
	}

	switch e.Name {
	case "mounted":
		mountPath := (*e.Args)["mount-path"].(string)

		// what path are we starting at
		prefix := req.URL.Query().Get("Prefix")
		base := mountPath + "/__default__"

		// setting default limit to 100 files
		var maxKeys int64 = 100
		maxKeysStr := req.URL.Query().Get("MaxKeys")
		if maxKeysStr != "" {
			newLimit, err := strconv.Atoi(maxKeysStr)
			if err == nil {
				maxKeys = int64(newLimit)
			}
		}

		// default the page offset to 9
		var page int64 = 0
		pageStr := req.URL.Query().Get("Page")
		if pageStr != "" {
			newPage, err := strconv.Atoi(pageStr)
			if err == nil {
				page = int64(newPage)
			}
		}

		// default to recursive mode
		var recursive = true
		nonRecursiveStr := req.URL.Query().Get("NonRecursive")
		if nonRecursiveStr != "" {
			recursive = false
		}

		// default to not showing directories in the results
		var includeDirectories = false
		includeDirectoriesStr := req.URL.Query().Get("IncludeDirectories")
		if includeDirectoriesStr != "" {
			includeDirectories = true
		}

		// default to returning the results as XML
		// unless we ask for the format to be JSON
		var format = req.URL.Query().Get("Format")

		listFileRequest := types.ListFileRequest{
			Base:               base,
			Prefix:             prefix,
			MaxKeys:            maxKeys,
			Page:               page,
			Recursive:          recursive,
			IncludeDirectories: includeDirectories,
		}

		listFilesResponse, err := fsm.GetKeysForDirLimit(listFileRequest)
		if err != nil {
			http.Error(resp, "failed to get keys for dir: "+err.Error(), 500)
			l.WithError(err).Error("[S3Handler.listBucket] failed to get keys for dir")
			return
		}

		bucket := ListBucketResult{
			Name:         name,
			Prefix:       prefix,
			Contents:     listFilesResponse.Items,
			TotalResults: listFilesResponse.TotalCount,
		}

		if format == "json" {
			resp.Header().Set("Content-Type", "application/json")
			resp.WriteHeader(200)

			// Set up the pipe to write data directly into the Reader.
			pr, pw := io.Pipe()

			// Write JSON-encoded data to the Writer end of the pipe.
			// Write in a separate concurrent goroutine, and remember
			// to Close the PipeWriter, to signal to the paired PipeReader
			// that we’re done writing.
			go func() {
				pw.CloseWithError(json.NewEncoder(pw).Encode(bucket))
			}()

			io.Copy(resp, pr)
		} else {
			enc := xml.NewEncoder(resp)
			err = enc.Encode(&bucket)
			if err != nil {
				http.Error(resp, fmt.Sprintf("failed to marshal response body: %s", err), 500)
				l.WithError(err).Error("[S3Handler.listBucket] failed to marshal response body")
			}
		}

		return

	case "no-snapshots-found":
		http.Error(resp, "No snaps to mount - commit before listing.", 400)
		l.Error("[S3Handler.listBucket] No snaps to mount")
	default:
		log.WithFields(log.Fields{
			"event":      e,
			"filesystem": filesystemId,
		}).Error("mount failed, returned event is not 'mounted'")
		l.WithField("event", fmt.Sprintf("%#v", e)).Error("[S3Handler.listBucket] failed to mount")
		http.Error(resp, fmt.Sprintf("failed to mount filesystem (%s), error: %s", e.Name, e.Error()), 500)
	}
}
