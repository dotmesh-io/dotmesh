package client

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"golang.org/x/net/context"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const DEFAULT_BRANCH string = "master"
const RPC_TIMEOUT time.Duration = 20 * time.Second

type VersionInfo struct {
	InstalledVersion    string `json:"installed_version"`
	CurrentVersion      string `json:"current_version"`
	CurrentReleaseDate  int    `json:"current_release_date"`
	CurrentDownloadURL  string `json:"current_download_url"`
	CurrentChangelogURL string `json:"current_changelog_url"`
	ProjectWebsite      string `json:"project_website"`
	Outdated            bool   `json:"outdated"`
}

type DotmeshAPI struct {
	Configuration *Configuration
	configPath    string
	Client        *JsonRpcClient
	verbose       bool
}

type Dotmesh interface {
	CallRemote(ctx context.Context, method string, args interface{}, response interface{}) error
	ListCommits(activeVolumeName, activeBranch string) ([]types.Snapshot, error)
	CommitsById(dotID string) ([]types.Snapshot, error)
	GetFsId(namespace, name, branch string) (string, error)
	Get(fsId string) (types.DotmeshVolume, error)
	Procure(data types.ProcureArgs) (string, error)
	CommitWithStruct(args types.CommitArgs) (string, error)
	NewVolumeFromStruct(name types.VolumeName) (bool, error)
	GetMasterBranchId(volume types.VolumeName) (string, error)
	DeleteVolumeFromStruct(name types.VolumeName) (bool, error)
	MountCommit(request types.MountCommitRequest) (string, error)
	Rollback(request types.RollbackRequest) (bool, error)
	Fork(request types.ForkRequest) (string, error)
	List() (map[string]map[string]types.DotmeshVolume, error)
	GetVersion() (VersionInfo, error)
	GetTransfer(transferId string) (TransferPollResult, error)
}

func CheckName(name string) bool {
	// TODO add more checks around sensible names?
	return len(name) <= 50
}

func NewDotmeshAPI(configPath string, verbose bool) (*DotmeshAPI, error) {
	c, err := NewConfiguration(configPath)
	if err != nil {
		return nil, err
	}

	d := &DotmeshAPI{
		Configuration: c,
		Client:        nil,
		verbose:       verbose,
	}
	return d, nil
}

func NewDotmeshAPIFromClient(client *JsonRpcClient, verbose bool) *DotmeshAPI {
	return &DotmeshAPI{
		Configuration: nil,
		Client:        client,
		verbose:       verbose,
	}
}

func (dm *DotmeshAPI) openClient() error {
	if dm.Client == nil {
		client, err := dm.Configuration.ClusterFromCurrentRemote(dm.verbose)
		if err == nil {
			dm.Client = client
			return nil
		} else {
			return err
		}
	} else {
		return nil
	}
}

// proxy thru
func (dm *DotmeshAPI) CallRemote(
	ctx context.Context, method string, args interface{}, response interface{},
) error {
	err := dm.openClient()
	if err == nil {
		return dm.Client.CallRemote(ctx, method, args, response)
	} else {
		return err
	}
}

func (dm *DotmeshAPI) List() (map[string]map[string]types.DotmeshVolume, error) {
	filesystems := make(map[string]map[string]types.DotmeshVolume)
	err := dm.CallRemote(
		context.Background(), "DotmeshRPC.List", nil, &filesystems,
	)
	return filesystems, err
}

func (dm *DotmeshAPI) Fork(request types.ForkRequest) (string, error) {
	var forkDotId string
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Fork", request, &forkDotId)
	return forkDotId, err
}

func (dm *DotmeshAPI) GetMasterBranchId(volume types.VolumeName) (string, error) {
	var masterBranchId string
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Exists", &volume, &masterBranchId)
	return masterBranchId, err
}

func (dm *DotmeshAPI) MountCommit(request types.MountCommitRequest) (string, error) {
	var mountpoint string
	err := dm.CallRemote(context.Background(), "DotmeshRPC.MountCommit", request, &mountpoint)
	return mountpoint, err
}

func (dm *DotmeshAPI) PingLocal() (bool, error) {
	err := dm.openClient()
	if err == nil {
		return dm.Client.Ping()
	} else {
		return false, err
	}
}

func (dm *DotmeshAPI) BackupEtcd() (string, error) {
	var response types.BackupV1
	err := dm.CallRemote(context.Background(), "DotmeshRPC.DumpEtcd",
		struct{ Prefix string }{Prefix: ""},
		&response,
	)
	if err != nil {
		return "", err
	}

	bts, err := json.Marshal(&response)
	if err != nil {
		return "", err
	}

	return string(bts), nil
}

func (dm *DotmeshAPI) RestoreEtcd(dump string) error {
	var response bool
	err := dm.CallRemote(context.Background(), "DotmeshRPC.RestoreEtcd",
		struct {
			Prefix string
			Dump   string
		}{
			Prefix: "",
			Dump:   dump,
		},
		&response,
	)
	if err != nil {
		return err
	}
	return nil
}

func (dm *DotmeshAPI) GetVersion() (VersionInfo, error) {
	var response VersionInfo
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Version", struct{}{}, &response)
	if err != nil {
		return VersionInfo{}, err
	}
	return response, nil
}

func (dm *DotmeshAPI) Get(FsID string) (types.DotmeshVolume, error) {
	volume := types.DotmeshVolume{}
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Get", FsID, &volume)
	if err != nil {
		return types.DotmeshVolume{}, err
	}
	return volume, nil
}

func (dm *DotmeshAPI) NewVolume(volumeName string) error {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return err
	}
	sendVolumeName := types.VolumeName{
		Namespace: namespace,
		Name:      name,
	}
	_, err = dm.NewVolumeFromStruct(sendVolumeName)
	if err != nil {
		return err
	}
	return dm.setCurrentVolume(volumeName)
}

func (dm *DotmeshAPI) NewVolumeFromStruct(name types.VolumeName) (bool, error) {
	var response bool
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Create", name, &response)
	if err != nil {
		return false, err
	}
	return response, nil
}

func (dm *DotmeshAPI) ProcureVolume(volumeName string) (string, error) {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return "", err
	}
	sendVolumeName := types.ProcureArgs{
		Namespace: namespace,
		Name:      name,
		Subdot:    "__default__",
	}
	return dm.Procure(sendVolumeName)
}

func (dm *DotmeshAPI) Procure(data types.ProcureArgs) (string, error) {
	var response string
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Procure", data, &response)
	return response, err
}

func (dm *DotmeshAPI) setCurrentVolume(volumeName string) error {
	return dm.Configuration.SetCurrentVolume(volumeName)
}

func (dm *DotmeshAPI) setCurrentBranch(volumeName, branchName string) error {
	// TODO make an API call here to switch running point for containers
	return dm.Configuration.SetCurrentBranchForVolume(volumeName, branchName)
}

func (dm *DotmeshAPI) CreateBranch(volumeName, sourceBranch, newBranch string) error {
	var result bool

	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return err
	}

	commitId, err := dm.findCommit("HEAD", volumeName, sourceBranch)
	if err != nil {
		return err
	}

	return dm.CallRemote(
		context.Background(),
		"DotmeshRPC.Branch",
		struct {
			// Create a named clone from a given volume+branch pair at a given
			// commit (that branch's latest commit)
			Namespace, Name, SourceBranch, NewBranchName, SourceCommitId string
		}{
			Namespace:      namespace,
			Name:           name,
			SourceBranch:   sourceBranch,
			SourceCommitId: commitId,
			NewBranchName:  newBranch,
		},
		&result,
	)
	/*
		TODO (maybe distinguish between `dm checkout -b` and `dm branch` based
		on whether or not we switch the active branch here)
		return dm.setCurrentBranch(volumeName, branchName)
	*/
}

func (dm *DotmeshAPI) CheckoutBranch(volumeName, from, to string, create bool) error {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return err
	}

	exists, err := dm.BranchExists(volumeName, to)
	if err != nil {
		return err
	}
	// The DEFAULT_BRANCH always implicitly exists
	exists = exists || to == DEFAULT_BRANCH
	if create {
		if exists {
			return fmt.Errorf("Branch already exists: %s", to)
		}
		if err := dm.CreateBranch(volumeName, from, to); err != nil {
			return err
		}
	}
	if !create {
		if !exists {
			return fmt.Errorf("Branch does not exist: %s", to)
		}
	}
	if err := dm.setCurrentBranch(volumeName, to); err != nil {
		return err
	}
	var result bool
	err = dm.CallRemote(context.Background(),
		"DotmeshRPC.SwitchContainers", map[string]string{
			"Namespace":     namespace,
			"Name":          name,
			"NewBranchName": deMasterify(to),
		}, &result)
	if err != nil {
		return err
	}
	return nil
}

func (dm *DotmeshAPI) StrictCurrentVolume() (string, error) {
	cv, err := dm.CurrentVolume()

	if err != nil {
		return "", err
	}

	if cv == "" {
		return "", fmt.Errorf("No current dot is selected. List them with 'dm list' and select one with 'dm switch'.")
	}

	return cv, nil
}

func (dm *DotmeshAPI) CurrentVolume() (string, error) {
	cv, err := dm.Configuration.CurrentVolume()

	if err != nil {
		return "", err
	}

	if cv == "" {
		// No default volume has been specified, let's see if we can pick a default
		vs, err := dm.AllVolumes()
		if err != nil {
			return "", err
		}

		// Only one volume around? Let's default to that.
		if len(vs) == 1 {
			cv = vs[0].Name.String()
			// Preserve this for future

			dm.setCurrentVolume(cv)
		}
	}

	return cv, nil
}

func (dm *DotmeshAPI) Rollback(request types.RollbackRequest) (bool, error) {
	var result bool
	err := dm.CallRemote(context.Background(), "DotmeshRPC.Rollback", request, &result)
	return result, err
}

func (dm *DotmeshAPI) BranchExists(volumeName, branchName string) (bool, error) {
	branches, err := dm.Branches(volumeName)
	if err != nil {
		return false, err
	}
	for _, branch := range branches {
		if branch == branchName {
			return true, nil
		}
	}
	return false, nil
}

func (dm *DotmeshAPI) Branches(volumeName string) ([]string, error) {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return []string{}, err
	}

	branches := []string{}
	err = dm.CallRemote(
		context.Background(), "DotmeshRPC.Branches", types.VolumeName{namespace, name}, &branches,
	)
	if err != nil {
		return []string{}, err
	}
	return branches, nil
}

func (dm *DotmeshAPI) VolumeExists(volumeName string) (bool, error) {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return false, err
	}

	volumes, err := dm.List()
	if err != nil {
		return false, err
	}

	volumesInNamespace, ok := volumes[namespace]
	if !ok {
		return false, nil
	}
	_, ok = volumesInNamespace[name]
	return ok, nil
}

func (dm *DotmeshAPI) DeleteVolume(volumeName string) error {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return err
	}

	_, err = dm.DeleteVolumeFromStruct(types.VolumeName{
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return err
	}

	if dm.Configuration != nil {
		err = dm.Configuration.DeleteStateForVolume(volumeName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dm *DotmeshAPI) DeleteVolumeFromStruct(name types.VolumeName) (bool, error) {
	var result bool
	err := retryUntilSucceeds(func() error {
		err := dm.CallRemote(
			context.Background(), "DotmeshRPC.Delete", name, &result,
		)
		if err != nil {
			return err
		}
		return nil
	}, 5, 1*time.Second)
	return result, err
}

func retryUntilSucceeds(f func() error, retries int, delay time.Duration) error {
	var err error
	for try := 0; try < retries; try++ {
		err = f()
		if err != nil {
			time.Sleep(time.Duration(try) * delay)
		} else {
			return nil
		}
	}
	return err
}

func (dm *DotmeshAPI) GetReplicationLatencyForBranch(volumeName string, branch string) (map[string][]string, error) {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return nil, err
	}

	var result map[string][]string
	err = dm.CallRemote(
		context.Background(), "DotmeshRPC.GetReplicationLatencyForBranch",
		struct {
			Namespace, Name, Branch string
		}{
			Namespace: namespace,
			Name:      name,
			Branch:    branch,
		},
		&result,
	)

	return result, err
}

func (dm *DotmeshAPI) SwitchVolume(volumeName string) error {
	return dm.setCurrentVolume(volumeName)
}

func (dm *DotmeshAPI) CurrentBranch(volumeName string) (string, error) {
	return dm.Configuration.CurrentBranchFor(volumeName)
}

func (dm *DotmeshAPI) AllBranches(volumeName string) ([]string, error) {
	namespace, name, err := ParseNamespacedVolume(volumeName)
	if err != nil {
		return []string{}, err
	}

	var branches []string
	err = dm.CallRemote(
		context.Background(), "DotmeshRPC.Branches", types.VolumeName{namespace, name}, &branches,
	)
	// the "main" filesystem (topLevelFilesystemId) is the master branch
	// (DEFAULT_BRANCH)
	branches = append(branches, DEFAULT_BRANCH)
	sort.Strings(branches)
	return branches, err
}

func (dm *DotmeshAPI) GetFsId(namespace, name, branch string) (string, error) {
	var fsId string
	err := dm.CallRemote(
		context.Background(), "DotmeshRPC.Lookup", struct{ Namespace, Name, Branch string }{
			Namespace: namespace,
			Name:      name,
			Branch:    branch},
		&fsId,
	)
	if err != nil {
		return "", err
	}
	return fsId, nil
}

func (dm *DotmeshAPI) BranchInfo(namespace, name, branch string) (types.DotmeshVolume, error) {
	var fsId string
	err := dm.CallRemote(
		context.Background(), "DotmeshRPC.Lookup", struct{ Namespace, Name, Branch string }{
			Namespace: namespace,
			Name:      name,
			Branch:    branch},
		&fsId,
	)
	if err != nil {
		return types.DotmeshVolume{}, err
	}

	return dm.Get(fsId)
}

func (dm *DotmeshAPI) ForceBranchMaster(namespace, name, branch, newMaster string) error {
	var fsId string
	err := dm.CallRemote(
		context.Background(), "DotmeshRPC.Lookup", struct{ Namespace, Name, Branch string }{
			Namespace: namespace,
			Name:      name,
			Branch:    branch},
		&fsId,
	)
	if err != nil {
		return err
	}

	var result bool
	err = dm.CallRemote(
		context.Background(), "DotmeshRPC.ForceBranchMasterById", struct{ FilesystemId, Master string }{
			FilesystemId: fsId,
			Master:       newMaster,
		}, &result,
	)
	return err
}

func (dm *DotmeshAPI) AllVolumes() ([]types.DotmeshVolume, error) {
	result := []types.DotmeshVolume{}
	interim := map[string]types.DotmeshVolume{}
	filesystems, err := dm.List()
	if err != nil {
		return result, err
	}
	names := []string{}
	for namespace, volumesInNamespace := range filesystems {
		var prefix string
		if namespace == "admin" {
			prefix = ""
		} else {
			prefix = namespace + "/"
		}

		for filesystem, v := range volumesInNamespace {
			name := prefix + filesystem
			interim[name] = v
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		result = append(result, interim[name])
	}
	return result, nil
}

func deMasterify(s string) string {
	// use empty string to indicate "no clone"
	if s == DEFAULT_BRANCH {
		return ""
	}
	return s
}

type CommitArgs struct {
	Namespace string
	Name      string
	Branch    string
	Message   string
	Metadata  map[string]string
}

func (dm *DotmeshAPI) Commit(activeVolumeName, activeBranch, commitMessage string, metadata map[string]string) (string, error) {
	activeNamespace, activeVolume, err := ParseNamespacedVolume(activeVolumeName)
	if err != nil {
		return "", err
	}
	args := types.CommitArgs{
		Namespace: activeNamespace,
		Name:      activeVolume,
		Branch:    deMasterify(activeBranch),
		Message:   commitMessage,
		Metadata:  metadata,
	}
	return dm.CommitWithStruct(args)
}

func (dm *DotmeshAPI) CommitWithStruct(args types.CommitArgs) (string, error) {
	var result string
	err := dm.CallRemote(
		context.Background(),
		"DotmeshRPC.Commit",
		&args,
		&result,
	)
	return result, err
}

func (dm *DotmeshAPI) ListCommits(activeVolumeName, activeBranch string) ([]types.Snapshot, error) {
	var result []types.Snapshot

	activeNamespace, activeVolume, err := ParseNamespacedVolume(activeVolumeName)
	if err != nil {
		return []types.Snapshot{}, err
	}

	err = dm.CallRemote(
		context.Background(),
		"DotmeshRPC.Commits",
		map[string]string{
			"Namespace": activeNamespace,
			"Name":      activeVolume,
			"Branch":    deMasterify(activeBranch),
		},
		// TODO recusively prefix clones' origin snapshots (but error on
		// resetting to them, and maybe mark the origin snap in a particular
		// way in the 'dm log' output)
		&result,
	)
	if err != nil {
		return []types.Snapshot{}, err
	}
	return result, nil
}

func (dm *DotmeshAPI) CommitsById(dotID string) ([]types.Snapshot, error) {
	var commits []types.Snapshot

	err := dm.CallRemote(context.Background(), "DotmeshRPC.CommitsById", dotID, &commits)
	return commits, err
}

func (dm *DotmeshAPI) findCommit(ref, volumeName, branchName string) (string, error) {
	hatRegex := regexp.MustCompile(`^HEAD\^*$`)
	if hatRegex.MatchString(ref) {
		countHats := len(ref) - len("HEAD")
		cs, err := dm.ListCommits(volumeName, branchName)
		if err != nil {
			return "", err
		}
		if len(cs) == 0 {
			return "", fmt.Errorf("No commits match %s", ref)
		}
		i := len(cs) - 1 - countHats
		if i < 0 {
			return "", fmt.Errorf("Commits don't go back that far")
		}
		return cs[i].Id, nil
	} else {
		return ref, nil
	}
}

func (dm *DotmeshAPI) ResetCurrentVolume(commit string) error {
	activeVolume, err := dm.CurrentVolume()
	if err != nil {
		return err
	}

	if activeVolume == "" {
		return fmt.Errorf("No current volume is selected. List them with 'dm list' and select one with 'dm switch'.")
	}

	namespace, name, err := ParseNamespacedVolume(activeVolume)
	if err != nil {
		return err
	}

	activeBranch, err := dm.CurrentBranch(activeVolume)
	if err != nil {
		return err
	}
	var result bool
	commitId, err := dm.findCommit(commit, activeVolume, activeBranch)
	if err != nil {
		return err
	}
	err = dm.CallRemote(
		context.Background(),
		"DotmeshRPC.Rollback",
		map[string]string{
			"Namespace":  namespace,
			"Name":       name,
			"Branch":     deMasterify(activeBranch),
			"SnapshotId": commitId,
		},
		&result,
	)
	if err != nil {
		return err
	}
	return nil
}

type Container struct {
	Id   string
	Name string
}

type DotmeshVolumeAndContainers struct {
	Volume     types.DotmeshVolume
	Containers []Container
}

func (dm *DotmeshAPI) AllVolumesWithContainers() ([]DotmeshVolumeAndContainers, error) {
	filesystems := map[string]map[string]DotmeshVolumeAndContainers{}
	result := []DotmeshVolumeAndContainers{}
	interim := map[string]DotmeshVolumeAndContainers{}
	err := dm.CallRemote(
		context.Background(), "DotmeshRPC.ListWithContainers", nil, &filesystems,
	)
	if err != nil {
		return result, err
	}
	names := []string{}
	for namespace, volumesInNamespace := range filesystems {
		var prefix string
		if namespace == "admin" {
			prefix = ""
		} else {
			prefix = namespace + "/"
		}

		for filesystem, v := range volumesInNamespace {
			name := prefix + filesystem
			interim[name] = v
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		result = append(result, interim[name])
	}
	return result, nil
}

func (dm *DotmeshAPI) RelatedContainers(volumeName types.VolumeName, branch string) ([]Container, error) {
	result := []Container{}
	err := dm.CallRemote(
		context.Background(),
		"DotmeshRPC.Containers",
		map[string]string{
			"Namespace": volumeName.Namespace,
			"Name":      volumeName.Name,
			"Branch":    deMasterify(branch),
		},
		&result,
	)
	if err != nil {
		return []Container{}, err
	}
	return result, nil
}

type TransferPollResult struct {
	TransferRequestId string
	Direction         string // "push" or "pull"

	// Same across both clusters
	FilesystemId string

	// TODO add clusterIds? probably comes from etcd. in fact, could be the
	// discovery id (although that is only for bootstrap... hmmm).
	InitiatorNodeId string
	PeerNodeId      string

	// XXX a Transfer that spans multiple filesystem ids won't have a unique
	// starting/target snapshot, so this is in the wrong place right now.
	// although maybe it makes sense to talk about a target *final* snapshot,
	// with interim snapshots being an implementation detail.
	StartingSnapshot string
	TargetSnapshot   string

	Index              int    // i.e. transfer 1/4 (Index=1)
	Total              int    //                   (Total=4)
	Status             string // one of "starting", "running", "finished", "error"
	NanosecondsElapsed int64
	Size               int64 // size of current segment in bytes
	Sent               int64 // number of bytes of current segment sent so far
	Message            string
}

func (transferPollResult TransferPollResult) String() string {
	v := reflect.ValueOf(transferPollResult)
	protectedValue := "****"
	toString := "TransferPollResult : "
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, protectedValue)
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

func (dm *DotmeshAPI) GetTransfer(transferId string) (TransferPollResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
	return dm.GetTransferWithContext(transferId, ctx, cancel)
}

func (dm *DotmeshAPI) GetTransferWithContext(transferId string, ctx context.Context, cancel context.CancelFunc) (TransferPollResult, error) {
	defer cancel()
	var result TransferPollResult
	err := dm.CallRemote(
		ctx, "DotmeshRPC.GetTransfer", transferId, &result,
	)
	return result, err
}

type pollTransferInternalResult struct {
	result TransferPollResult
	err    error
}

func (dm *DotmeshAPI) PollTransfer(transferId string, out io.Writer) error {

	out.Write([]byte("Calculating...\n"))

	var bar *pb.ProgressBar
	started := false

	debugMode := os.Getenv("DEBUG_MODE") != ""
	for {
		if debugMode {
			out.Write([]byte("DEBUG About to sleep for 1s...\n"))
		}
		time.Sleep(time.Second)

		rpcResult := make(chan pollTransferInternalResult, 1)

		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		defer cancel()
		go func() {
			if debugMode {
				out.Write([]byte(fmt.Sprintf("DEBUG Calling GetTransfer(%s)...\n", transferId)))
			}
			var result pollTransferInternalResult
			result.result, result.err = dm.GetTransferWithContext(transferId, ctx, cancel)
			if debugMode {
				out.Write([]byte(fmt.Sprintf(
					"DEBUG done GetTransfer(%s), got err %#v and result %#v...\n",
					transferId, result.err, result.result,
				)))
			}
			rpcResult <- result
			if debugMode {
				out.Write([]byte("DEBUG rpcResult consumed!\n"))
			}
		}()

		if debugMode {
			out.Write([]byte("DEBUG About to select...\n"))
		}
		var result pollTransferInternalResult
		select {
		case <-ctx.Done():
			out.Write([]byte(fmt.Sprintf("Got timeout error from API, trying again: %s\n", ctx.Err())))

			// Asynchronoulsy discard rpcResult's message when it arrives, so as to not leak the goroutine.
			go func() {
				<-rpcResult
			}()

			// Proceed with result at its default value.
		case result = <-rpcResult:
			if debugMode {
				out.Write([]byte(fmt.Sprintf("DEBUG Got result: %s / %#v\n", result.err, result.result)))
			}
			if result.err != nil {
				// Suppress display of "No such intercluster transfer" errors,
				// which we get at the start before the transfer has started.
				if !strings.Contains(fmt.Sprintf("%s", result.err), "No such intercluster transfer") {
					out.Write([]byte(fmt.Sprintf("Got error, trying again: %s\n", result.err)))
				}
			}
		}

		if !started {
			bar = pb.New64(result.result.Size)
			bar.ShowFinalTime = false
			bar.SetMaxWidth(80)
			bar.SetUnits(pb.U_BYTES)
			bar.Start()
			started = true
		}

		if result.result.Size != 0 {
			bar.Total = result.result.Size
		}
		// Numbers reported by data transferred thru dotmesh versus size
		// of stream reported by 'zfs send -nP' are off by a few kilobytes,
		// fudge it (maybe no one will notice).
		if result.result.Sent > result.result.Size {
			bar.Set64(result.result.Size)
		} else {
			bar.Set64(result.result.Sent)
		}
		bar.Prefix(result.result.Status)
		var speed string
		if result.result.NanosecondsElapsed > 0 {
			speed = fmt.Sprintf(" %.2f MiB/s",
				// mib/sec
				(float64(result.result.Sent)/(1024*1024))/
					(float64(result.result.NanosecondsElapsed)/(1000*1000*1000)),
			)
		} else {
			speed = " ? MiB/s"
		}
		quotient := fmt.Sprintf(" (%d/%d)", result.result.Index, result.result.Total)
		bar.Postfix(speed + quotient)

		if debugMode {
			out.Write([]byte(fmt.Sprintf("DEBUG status %d / %d : %s\n", result.result.Index, result.result.Total, result.result.Status)))
		}

		if result.result.Index == result.result.Total && result.result.Status == "finished" {
			if started {
				bar.FinishPrint("Done!")
			}
			// A terrible hack: many of the tests race the next 'dm log' or
			// similar command against snapshots received by a push/pull/clone
			// updating etcd which updates nodes' local caches of state. Give
			// the etcd updates a 1 second head start, which might reduce the
			// incidence of test flakes.
			// TODO: In general, we need a better way to _request_ the current
			// state of snapshots on a node, rather than always consulting
			// potentially out-of-date global caches. This might help with
			// scaling, too.
			time.Sleep(time.Second)
			return nil
		}
		if result.result.Status == "error" {
			if started {
				bar.FinishPrint(fmt.Sprintf("error: %s", result.result.Message))
			}
			out.Write([]byte(result.result.Message + "\n"))
			// A similarly terrible hack. See comment above.
			time.Sleep(time.Second)
			return fmt.Errorf(result.result.Message)
		}
	}
}

/*

pull
----

  to   from
  O*<-----O

push
----

  from   to
  O*----->O

* = current

*/

// attempt to get the latest commits in filesystemId (which may be a branch)
// from fromRemote to toRemote as a one-off.
//
// the reason for supporting both directions is that the "current" is often
// behind NAT from its peer, and so it must initiate the connection.
func (dm *DotmeshAPI) RequestTransfer(
	direction, peer,
	localFilesystemName, localBranchName,
	remoteFilesystemName, remoteBranchName string,
	prefixes []string,
	stashDivergence bool,
) (string, error) {
	connectionInitiator := dm.Configuration.CurrentRemote

	debugMode := os.Getenv("DEBUG_MODE") != ""

	if debugMode {
		fmt.Printf("[DEBUG RequestTransfer] dir=%s peer=%s lfs=%s lb=%s rfs=%s rb=%s\n",
			direction, peer,
			localFilesystemName, localBranchName,
			remoteFilesystemName, remoteBranchName)
	}

	var err error

	remote, err := dm.Configuration.GetRemote(peer)
	if err != nil {
		return "", err
	}

	// Let's replace any missing things with defaults.
	// The defaults depend on whether we're pushing or pulling.
	if direction == "push" {
		// We are pushing, so if no local filesystem/branch is
		// specified, take the current one.
		if localFilesystemName == "" {
			localFilesystemName, err = dm.Configuration.CurrentVolume()
			if err != nil {
				return "", err
			}
		}

		if localBranchName == "" {
			localBranchName, err = dm.Configuration.CurrentBranch()
			if err != nil {
				return "", err
			}
		}
	} else if direction == "pull" {
		// We are pulling, so if no local filesystem/branch is
		// specified, we take the remote name but strip it of its
		// namespace. So if we pull "bob/apples", we pull into "apples",
		// which is really "admin/apples".
		if localFilesystemName == "" && remoteFilesystemName != "" {
			_, localFilesystemName, err = ParseNamespacedVolume(remoteFilesystemName)
			if err != nil {
				return "", err
			}
		}
	}

	// Split the local volume name's namespace out
	localNamespace, localVolume, err := ParseNamespacedVolume(localFilesystemName)
	if err != nil {
		return "", err
	}

	// Guess defaults for the remote filesystem
	var remoteNamespace, remoteVolume string
	if remoteFilesystemName == "" {
		// No remote specified. Do we already have a default configured?
		defaultRemoteNamespace, defaultRemoteVolume, ok := dm.Configuration.DefaultRemoteVolumeFor(peer, localNamespace, localVolume)
		if ok {
			// If so, use it
			remoteNamespace = defaultRemoteNamespace
			remoteVolume = defaultRemoteVolume
		} else {
			// If not, default to the un-namespaced local filesystem name.
			// This causes it to default into the user's own namespace
			// when we parse the name, too.
			remoteNamespace = remote.DefaultNamespace()
			remoteVolume = localVolume
		}
	} else {
		// Default namespace for remote volume is the username on this remote
		remoteNamespace, remoteVolume, err = ParseNamespacedVolumeWithDefault(remoteFilesystemName, remote.DefaultNamespace())
		if err != nil {
			return "", err
		}
	}

	// Remember default remote if there isn't already one
	_, _, ok := dm.Configuration.DefaultRemoteVolumeFor(peer, localNamespace, localVolume)
	if !ok {
		dm.Configuration.SetDefaultRemoteVolumeFor(peer, localNamespace, localVolume, remoteNamespace, remoteVolume)
	}

	if remoteBranchName == "" {
		remoteBranchName = localBranchName
	}

	if remoteBranchName != "" && remoteVolume == "" {
		return "", fmt.Errorf(
			"It's dubious to specify a remote branch name " +
				"without specifying a remote filesystem name.",
		)
	}

	if direction == "push" {
		fmt.Printf("Pushing %s/%s to %s:%s/%s\n",
			localNamespace, localVolume,
			peer,
			remoteNamespace, remoteVolume,
		)
	} else {
		fmt.Printf("Pulling %s/%s from %s:%s/%s\n",
			localNamespace, localVolume,
			peer,
			remoteNamespace, remoteVolume,
		)
	}

	// connect to connectionInitiator
	client, err := dm.Configuration.ClusterFromRemote(connectionInitiator, dm.verbose)
	if err != nil {
		return "", err
	}
	var transferId string
	// TODO make ApiKey time- and domain- (filesystem?) limited
	// cryptographically somehow
	dmRemote, ok := remote.(*DMRemote)

	if ok {
		transferRequest := types.TransferRequest{
			Peer:             dmRemote.Hostname,
			User:             dmRemote.User,
			Port:             dmRemote.Port,
			ApiKey:           dmRemote.ApiKey,
			Direction:        direction,
			LocalNamespace:   localNamespace,
			LocalName:        localVolume,
			LocalBranchName:  deMasterify(localBranchName),
			RemoteNamespace:  remoteNamespace,
			RemoteName:       remoteVolume,
			RemoteBranchName: deMasterify(remoteBranchName),
			StashDivergence:  stashDivergence,
			// TODO add TargetSnapshot here, to support specifying "push to a given
			// snapshot" rather than just "push all snapshots up to the latest"
		}

		if debugMode {
			fmt.Printf("[DEBUG] TransferRequest: %#v\n", transferRequest)
		}

		err = client.CallRemote(context.Background(),
			"DotmeshRPC.Transfer", transferRequest, &transferId)
		if err != nil {
			return "", err
		}
	} else {
		s3Remote, ok := remote.(*S3Remote)
		if ok {
			if prefixes != nil {
				dm.Configuration.SetPrefixesFor(peer, localNamespace, localVolume, prefixes)
			}
			prefixes, _ = s3Remote.PrefixesFor(localNamespace, localVolume)
			transferRequest := types.S3TransferRequest{
				KeyID:           s3Remote.KeyID,
				SecretKey:       s3Remote.SecretKey,
				Endpoint:        s3Remote.Endpoint,
				Prefixes:        prefixes,
				Direction:       direction,
				LocalNamespace:  localNamespace,
				LocalName:       localVolume,
				LocalBranchName: deMasterify(localBranchName),
				RemoteName:      remoteVolume,
				// TODO add TargetSnapshot here, to support specifying "push to a given
				// snapshot" rather than just "push all snapshots up to the latest"
				// todo is stash divergence needed here?? (issue dotscience-agent#88)
			}

			if debugMode {
				fmt.Printf("[DEBUG] S3TransferRequest: %#v\n", transferRequest)
			}

			err = client.CallRemote(context.Background(),
				"DotmeshRPC.S3Transfer", transferRequest, &transferId)
			if err != nil {
				return "", err
			}
		} else {
			return "", fmt.Errorf("Unknown remote type %#v\n", remote)
		}
	}
	return transferId, nil

}

func (dm *DotmeshAPI) IsUserPriveledged() bool {
	err := dm.openClient()

	if err != nil {
		// No way to return errors from here...
		return false
	}

	if dm.Client.User == "admin" {
		return true
	}
	return false
}

type S3VolumeName struct {
	Namespace string
	Name      string
	Prefixes  []string
}

func ParseNamespacedVolumeWithDefault(name, defaultNamespace string) (string, string, error) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 0: // name was empty
		return "", "", nil
	case 1: // name was unqualified, no namespace, so we default
		return defaultNamespace, name, nil
	case 2: // Qualified name
		return parts[0], parts[1], nil
	default: // Too many slashes!
		return "", "", fmt.Errorf("Volume names must be of the form NAMESPACE/VOLUME or just VOLUME: '%s'", name)
	}
}

func ParseNamespacedVolume(name string) (string, string, error) {
	return ParseNamespacedVolumeWithDefault(name, "admin")
}
