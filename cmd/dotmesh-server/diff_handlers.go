package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"golang.org/x/net/context"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/dotmesh-io/dotmesh/pkg/validator"

	"github.com/gorilla/mux"

	log "github.com/sirupsen/logrus"
)

type DiffHandler struct {
	state *InMemoryState
	httputil.ReverseProxy
}

func NewDiffHandler(state *InMemoryState) http.Handler {
	h := &DiffHandler{
		state: state,
	}

	h.ReverseProxy.Director = h.Director

	return h
}

func (h *DiffHandler) Director(req *http.Request) {
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

func (s *DiffHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
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
		log.Warn("[DiffHandler.ServeHTTP] authentication failed")
		http.Error(resp, err.Error(), 401)
		return
	}
	if !isAdmin {
		errStr := fmt.Sprintf("User %s is not the administrator of namespace %s", auth.GetUserFromCtx(req.Context()).Name, volName.Namespace)
		log.Warn("[DiffHandler.ServeHTTP] " + errStr)
		http.Error(resp, errStr, 401)
		return
	}

	filesystemID := s.state.registry.Exists(volName, "")
	if err != nil {
		log.Warnf("[DiffHandler.ServeHTTP] filesystem '%s' not found", filesystemID)
		http.Error(resp, "filesystem not found", http.StatusPreconditionFailed)
		return
	}

	// ensure any of these requests end up on the current master node for
	// this filesystem
	master, err := s.state.registry.CurrentMasterNode(filesystemID)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("[DiffHandler.ServeHTTP] master node for filesystem not found")
		http.Error(resp, fmt.Sprintf("master node for filesystem %s not found", filesystemID), 500)
		return
	}
	if master != s.state.NodeID() {
		admin, err := s.state.userManager.Get(&user.Query{Ref: "admin"})
		if err != nil {
			http.Error(resp, fmt.Sprintf("Can't get API key to proxy s3 request: %+v.\n", err), 500)
			log.Errorf("can't get API key to proxy s3: %+v.", err)
			return
		}
		addresses := s.state.AddressesForServer(master)
		target, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey) // FIXME, need master->name mapping, see how handover works normally
		if err != nil {
			http.Error(resp, err.Error(), 500)
			log.Errorf("can't establish URL to proxy s3: %+v.", err)
			return
		}
		log.Infof("[DiffHandler.ServeHTTP] proxying PUT request to node: %s", target)
		s.ReverseProxy.ServeHTTP(resp, req.WithContext(ctxSetAddress(req.Context(), target)))
		return
	}

	snapshotID, ok := vars["snapshotID"]
	if !ok || snapshotID == "" {
		snapshots, err := s.state.SnapshotsForCurrentMaster(filesystemID)
		if err != nil {
			http.Error(resp, fmt.Sprintf("failed to retrieve snapshots: %s", err), http.StatusInternalServerError)
			return
		}
		if len(snapshots) == 0 {
			http.Error(resp, "no snapshots found", http.StatusBadRequest)
			return
		}

		snapshotID = snapshots[len(snapshots)-1].Id
	}
	if !validator.EnsureValidOrRespond(snapshotID, validator.IsValidSnapshotName, resp) {
		return
	}

	// node is local, proceed with zfs diff
	diff, err := s.getDiff(filesystemID, snapshotID)
	if err != nil {
		http.Error(resp, err.Error(), 500)
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(200)
	json.NewEncoder(resp).Encode(&diff)
}

func (s *DiffHandler) getDiff(filesystemID, snapshotID string) ([]types.ZFSFileDiff, error) {

	fsm, err := s.state.InitFilesystemMachine(filesystemID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize filesystem machine: %s", err)
	}

	if fsm.GetCurrentState() != "active" {
		return nil, fmt.Errorf("filesystem not ready, please try again later")
	}

	responseChan, err := s.state.globalFsRequest(
		filesystemID,
		&Event{Name: "diff",
			Args: &EventArgs{"snapshot_id": snapshotID}},
	)
	if err != nil {
		return nil, err
	}

	e := <-responseChan
	if e.Name == "diffed" {
		f, ok := (*e.Args)["files"]
		if !ok {
			return nil, fmt.Errorf("no files returned")
		}

		encodedFiles, ok := f.(string)
		if !ok {
			return nil, fmt.Errorf("interface conversion failed to files: %v", f)
		}

		files, err := types.DecodeZFSFileDiff(encodedFiles)
		if err != nil {
			return nil, fmt.Errorf("failed to decode zfs diff files: %s", err)
		}

		return files, nil
	}
	return nil, fmt.Errorf("diff failed: %s", err)

}
