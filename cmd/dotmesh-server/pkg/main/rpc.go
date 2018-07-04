package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// TODO ensure contexts are threaded through in all RPC calls for correct
// authorization.

// rpc server
type DotmeshRPC struct {
	state *InMemoryState
}

func NewDotmeshRPC(state *InMemoryState) *DotmeshRPC {
	return &DotmeshRPC{state: state}
}

var reNamespaceName = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)
var reVolumeName = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)
var reBranchName = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)
var reSubdotName = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

func requireValidVolumeName(name VolumeName) error {
	// Reject the request with an error if the volume name is invalid
	// This function allows only pure volume names - no volume@branch$subvolume or similar!

	if !reNamespaceName.MatchString(name.Namespace) {
		return fmt.Errorf("Invalid namespace name %v", name.Namespace)
	}

	if !reVolumeName.MatchString(name.Name) {
		return fmt.Errorf("Invalid dot name %v", name.Namespace)
	}

	return nil
}

func requireValidBranchName(name string) error {
	if name != "" && !reBranchName.MatchString(name) {
		return fmt.Errorf("Invalid branch name %v", name)
	}

	return nil
}

func requireValidSubdotName(name string) error {
	if !reSubdotName.MatchString(name) {
		return fmt.Errorf("Invalid subdot name %v", name)
	}

	return nil
}

func requireValidVolumeNameWithBranch(name VolumeName) error {
	// Reject the request with an error if the volume name is invalid.

	// This function allows pure volume names or ones with branches,
	// but does not allow subvolume syntax because that should have
	// been parsed out BEFORE we got into VolumeName territory.  The
	// only reason branch syntax leaks in here is because we've not
	// properly refactored the Procure API to accept a separate branch
	// name!

	if strings.Contains(name.Name, "@") {
		shrapnel := strings.Split(name.Name, "@")
		name.Name = shrapnel[0]

		err := requireValidBranchName(shrapnel[1])
		if err != nil {
			return err
		}
	}

	return requireValidVolumeName(name)
}

func ensureAdminUser(r *http.Request) error {
	requestId := r.Context().Value("authenticated-user-id").(string)

	// we have already authenticated the admin password so are safe to just compare ids
	if requestId != ADMIN_USER_UUID {
		log.Printf("Blocking access to API call by non-admin user: %+v", r)

		return fmt.Errorf(
			"Userid %s is not admin user",
			requestId,
		)
	}
	return nil
}

func (d *DotmeshRPC) Procure(
	r *http.Request, args *struct {
		Namespace string
		Name      string
		Subdot    string
	}, result *string) error {
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	ctx := r.Context()

	vn := VolumeName{args.Namespace, args.Name}

	err = requireValidVolumeNameWithBranch(vn)
	if err != nil {
		return err
	}

	err = requireValidSubdotName(args.Subdot)
	if err != nil {
		return err
	}

	filesystemId, err := d.state.procureFilesystem(ctx, vn)
	if err != nil {
		return err
	}
	mountpoint, err := newContainerMountSymlink(vn, filesystemId, args.Subdot)
	*result = mountpoint
	return err
}

func safeConfig(c Config) SafeConfig {
	safe := SafeConfig{}
	return safe
}

func (d *DotmeshRPC) Config(
	r *http.Request, args *struct{}, result *SafeConfig) error {
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	*result = safeConfig(d.state.config)
	return nil
}

func requirePassword(r *http.Request) error {
	// Reject the request with an error if the request was
	// authenticated with an API key rather than a password. Use this
	// to protect RPC methods that require a password.

	if r.Context().Value("password-authenticated").(bool) {
		return nil
	} else {
		return fmt.Errorf("Password authentication is required for this method.")
	}
}

func (d *DotmeshRPC) CurrentUser(
	r *http.Request, args *struct{}, result *SafeUser,
) error {
	user, err := GetUserById(r.Context().Value("authenticated-user-id").(string))
	if err != nil {
		return err
	}

	*result = safeUser(user)
	return nil
}

func (d *DotmeshRPC) ResetApiKey(
	r *http.Request, args *struct{}, result *struct{ ApiKey string },
) error {
	err := requirePassword(r)
	if err != nil {
		return err
	}

	user, err := GetUserById(r.Context().Value("authenticated-user-id").(string))
	if err != nil {
		return err
	}

	err = user.ResetApiKey()
	if err != nil {
		return err
	}

	err = user.Save()
	if err != nil {
		return err
	}

	result.ApiKey = user.ApiKey

	err = user.Save()
	if err != nil {
		return err
	}

	return nil
}

func (d *DotmeshRPC) GetApiKey(
	r *http.Request, args *struct{}, result *struct{ ApiKey string },
) error {
	user, err := GetUserById(r.Context().Value("authenticated-user-id").(string))
	if err != nil {
		return err
	}

	result.ApiKey = user.ApiKey
	return nil
}

// the user must have authenticated correctly with their old password in order
// to run this method
func (d *DotmeshRPC) UpdatePassword(
	r *http.Request, args *struct{ NewPassword string }, result *SafeUser,
) error {
	user, err := GetUserById(r.Context().Value("authenticated-user-id").(string))
	if err != nil {
		return err
	}
	user.UpdatePassword(args.NewPassword)
	err = user.Save()
	if err != nil {
		return err
	}
	*result = safeUser(user)
	return nil
}

// ADMIN BILLING FUNCTIONS

func (d *DotmeshRPC) RegisterNewUser(
	r *http.Request,
	args *struct{ Name, Email, Password string },
	result *SafeUser,
) error {

	// only admin can do this
	// TODO: we can turn this off and make it open access to allow JSON-RPC registrations
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	// validate
	if args.Password == "" {
		return fmt.Errorf("Password cannot be empty.")
	}
	if args.Email == "" {
		return fmt.Errorf("Email address cannot be empty.")
	}
	if args.Name == "" {
		return fmt.Errorf("Name cannot be empty.")
	} else if strings.Contains(args.Name, "/") {
		return fmt.Errorf("Invalid username.")
	}

	user, err := NewUser(args.Name, args.Email, args.Password)

	if err != nil {
		return fmt.Errorf("[RegistrationServer] Error creating user %v: %v", args.Name, err)
	} else {
		err = user.Save()
		if err != nil {
			return fmt.Errorf("[RegistrationServer] Error saving user %v: %v", args.Name, err)
		}
		*result = safeUser(user)
	}

	return nil
}

// update a users password given their id - admin only
func (d *DotmeshRPC) UpdateUserPassword(
	r *http.Request,
	args *struct {
		Id          string
		NewPassword string
	},
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}

	user, err := GetUserById(args.Id)
	if err != nil {
		return err
	}
	user.UpdatePassword(args.NewPassword)
	err = user.Save()
	if err != nil {
		return err
	}
	*result = safeUser(user)
	return nil
}

// given a stripe customerId - return the safeUser
func (d *DotmeshRPC) UserFromCustomerId(
	r *http.Request,
	args *struct{ CustomerId string },
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}
	user, err := GetUserByCustomerId(args.CustomerId)
	if err != nil {
		return err
	}

	*result = safeUser(user)
	return nil
}

func (d *DotmeshRPC) UserFromEmail(
	r *http.Request,
	args *struct{ Email string },
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}
	user, err := GetUserByEmail(args.Email)
	if err != nil {
		return err
	}

	*result = safeUser(user)
	return nil
}

func (d *DotmeshRPC) UserFromName(
	r *http.Request,
	args *struct{ Name string },
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}
	user, err := GetUserByName(args.Name)
	if err != nil {
		return err
	}

	*result = safeUser(user)
	return nil
}

// set a single value for the user Metadata
func (d *DotmeshRPC) SetUserMetadataField(
	r *http.Request,
	args *struct {
		Id    string
		Field string
		Value string
	},
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}
	user, err := GetUserById(args.Id)
	if err != nil {
		return err
	}

	user.Metadata[args.Field] = args.Value

	err = user.Save()
	if err != nil {
		return err
	}
	*result = safeUser(user)
	return nil
}

// delete a value for the user Metadata
func (d *DotmeshRPC) DeleteUserMetadataField(
	r *http.Request,
	args *struct {
		Id    string
		Field string
	},
	result *SafeUser,
) error {

	err := ensureAdminUser(r)
	if err != nil {
		return err
	}
	user, err := GetUserById(args.Id)
	if err != nil {
		return err
	}

	delete(user.Metadata, args.Field)

	err = user.Save()
	if err != nil {
		return err
	}
	*result = safeUser(user)
	return nil
}

// NORMAL USER API

func (d *DotmeshRPC) Get(
	r *http.Request, filesystemId *string, result *DotmeshVolume) error {
	v, err := d.state.getOne(r.Context(), *filesystemId)
	if err != nil {
		return err
	}
	*result = v
	return nil
}

// List all filesystems in the cluster.
func (d *DotmeshRPC) List(
	r *http.Request, args *struct{}, result *map[string]map[string]DotmeshVolume) error {
	log.Printf("[List] starting!")

	d.state.mastersCacheLock.Lock()
	filesystems := []string{}
	for fs, _ := range *d.state.mastersCache {
		filesystems = append(filesystems, fs)
	}
	d.state.mastersCacheLock.Unlock()

	gather := map[string]map[string]DotmeshVolume{}
	for _, fs := range filesystems {
		one, err := d.state.getOne(r.Context(), fs)
		// Just skip this in the result list if the context (eg authenticated
		// user) doesn't have permission to read it.
		if err != nil {
			switch err := err.(type) {
			case PermissionDenied:
				log.Printf("[List] permission denied reading %v", fs)
				continue
			default:
				log.Printf("[List] err: %v", err)
				// If we got an error looking something up, it might just be
				// because the fsMachine list or the registry is temporarily
				// inconsistent wrt the mastersCache. Proceed, at the risk of
				// lying slightly...
				continue
			}
		}
		submap, ok := gather[one.Name.Namespace]
		if !ok {
			submap = map[string]DotmeshVolume{}
			gather[one.Name.Namespace] = submap
		}

		submap[one.Name.Name] = one
	}
	log.Printf("[List] gather = %+v", gather)
	*result = gather
	return nil
}

// List all filesystems in the cluster.
func (d *DotmeshRPC) ListWithContainers(
	r *http.Request, args *struct{}, result *map[string]map[string]DotmeshVolumeAndContainers) error {
	log.Printf("[List] starting!")

	d.state.mastersCacheLock.Lock()
	filesystems := []string{}
	for fs, _ := range *d.state.mastersCache {
		filesystems = append(filesystems, fs)
	}
	d.state.mastersCacheLock.Unlock()

	d.state.globalContainerCacheLock.Lock()
	defer d.state.globalContainerCacheLock.Unlock()

	gather := map[string]map[string]DotmeshVolumeAndContainers{}
	for _, fs := range filesystems {
		one, err := d.state.getOne(r.Context(), fs)
		// Just skip this in the result list if the context (eg authenticated
		// user) doesn't have permission to read it.
		if err != nil {
			switch err := err.(type) {
			case PermissionDenied:
				log.Printf("[List] permission denied reading %v", fs)
				continue
			default:
				log.Printf("[List] err: %v", err)
				// If we got an error looking something up, it might just be
				// because the fsMachine list or the registry is temporarily
				// inconsistent wrt the mastersCache. Proceed, at the risk of
				// lying slightly...
				continue
			}
		}

		var containers []DockerContainer
		containerInfo, ok := (*d.state.globalContainerCache)[one.Id]
		if ok {
			containers = containerInfo.Containers
		} else {
			containers = []DockerContainer{}
		}

		submap, ok := gather[one.Name.Namespace]
		if !ok {
			submap = map[string]DotmeshVolumeAndContainers{}
			gather[one.Name.Namespace] = submap
		}

		submap[one.Name.Name] = DotmeshVolumeAndContainers{
			Volume:     one,
			Containers: containers,
		}
	}
	log.Printf("[List] gather = %+v", gather)
	*result = gather
	return nil
}

func (d *DotmeshRPC) Create(
	r *http.Request, filesystemName *VolumeName, result *bool) error {

	err := requireValidVolumeName(*filesystemName)
	if err != nil {
		return err
	}

	_, ch, err := d.state.CreateFilesystem(r.Context(), filesystemName)
	if err != nil {
		return err
	}
	e := <-ch
	if e.Name != "created" {
		return fmt.Errorf(
			"Could not create volume %s: unexpected response %s - %s",
			filesystemName, e.Name, e.Args,
		)
	}

	*result = true
	return nil
}

// Switch any containers which are currently using the given volume and clone
// name so that they use the new clone name by stopping them, changing the
// symlink, and starting them again.
func (d *DotmeshRPC) SwitchContainers(
	r *http.Request,
	args *struct{ Namespace, Name, NewBranchName string },
	result *bool,
) error {
	log.Printf("[SwitchContainers] being called with: %+v", args)

	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	err = requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.NewBranchName)
	if err != nil {
		return err
	}

	toFilesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name},
		args.NewBranchName,
	)
	if err != nil {
		return err
	}

	// Stop any other containers getting started until this function completes
	d.state.containersLock.Lock()
	defer d.state.containersLock.Unlock()

	// TODO Maybe be a bit more selective about which containers we stop/start
	// here (only ones which are using the given volume *and branch* name).
	if err := d.state.containers.Stop(args.Name); err != nil {
		log.Printf("[SwitchContainers] Error stopping containers: %+v", err)
		return err
	}
	err = d.state.containers.SwitchSymlinks(
		args.Name,
		mnt(toFilesystemId),
	)
	if err != nil {
		// TODO try to rollback (run Start)
		log.Printf("[SwitchContainers] Error switching symlinks: %+v", err)
		return err
	}

	*result = true
	err = d.state.containers.Start(args.Name)
	if err != nil {
		log.Printf("[SwitchContainers] Error starting containers: %+v", err)
	}
	return err
}

// Containers that were recently known to be running on a given filesystem.
func (d *DotmeshRPC) Containers(
	r *http.Request,
	args *struct{ Namespace, Name, Branch string },
	result *[]DockerContainer,
) error {
	log.Printf("[Containers] called with %+v", *args)

	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	filesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name},
		args.Branch,
	)
	if err != nil {
		log.Printf("[Containers] died of %#v", err)
		return err
	}
	d.state.globalContainerCacheLock.Lock()
	defer d.state.globalContainerCacheLock.Unlock()
	containerInfo, ok := (*d.state.globalContainerCache)[filesystemId]
	if !ok {
		*result = []DockerContainer{}
		return nil
	}
	// TODO maybe check that the server this containerInfo pertains to matches
	// what we believe the current master is, and otherwise flag to the
	// consumer of the API that the data may be stale
	*result = containerInfo.Containers
	return nil
}

// Containers that were recently known to be running on a given filesystem.
func (d *DotmeshRPC) ContainersById(
	r *http.Request,
	filesystemId *string,
	result *[]DockerContainer,
) error {
	d.state.globalContainerCacheLock.Lock()
	defer d.state.globalContainerCacheLock.Unlock()
	containerInfo, ok := (*d.state.globalContainerCache)[*filesystemId]
	if !ok {
		*result = []DockerContainer{}
		return nil
	}
	*result = containerInfo.Containers
	return nil
}

func (d *DotmeshRPC) Exists(
	r *http.Request,
	args *struct{ Namespace, Name, Branch string },
	result *string,
) error {
	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	fsId := d.state.registry.Exists(VolumeName{args.Namespace, args.Name}, args.Branch)
	deleted, err := isFilesystemDeletedInEtcd(fsId)
	if err != nil {
		return err
	}
	if deleted {
		*result = ""
	} else {
		*result = fsId
	}
	return nil
}

// TODO Dedupe this wrt Exists
func (d *DotmeshRPC) Lookup(
	r *http.Request,
	args *struct{ Namespace, Name, Branch string },
	result *string,
) error {
	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	filesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name}, args.Branch,
	)
	if err != nil {
		return err
	}
	deleted, err := isFilesystemDeletedInEtcd(filesystemId)
	if err != nil {
		return err
	}
	if deleted {
		*result = ""
	} else {
		*result = filesystemId
	}
	return nil
}

// Get a list of snapshots for a filesystem (or its specified clone). Snapshot
// objects have "id" and "metadata" fields, where id is an opaque, unique
// string and metadata is a mapping from strings to strings.
func (d *DotmeshRPC) Commits(
	r *http.Request,
	args *struct{ Namespace, Name, Branch string },
	result *[]snapshot,
) error {
	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	filesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name},
		args.Branch,
	)
	if err != nil {
		return err
	}
	snapshots, err := d.state.snapshotsForCurrentMaster(filesystemId)
	if err != nil {
		return err
	}
	*result = snapshots
	return nil
}

func (d *DotmeshRPC) CommitsById(
	r *http.Request,
	filesystemId *string,
	result *[]snapshot,
) error {
	snapshots, err := d.state.snapshotsForCurrentMaster(*filesystemId)
	if err != nil {
		return err
	}
	*result = snapshots
	return nil
}

// Acknowledge that an authenticated connection had been successfully established.
func (d *DotmeshRPC) Ping(r *http.Request, args *struct{}, result *bool) error {
	*result = true
	return nil
}

type CommitArgs struct {
	Namespace string
	Name      string
	Branch    string
	Message   string
	Metadata  metadata
}

// Take a snapshot of a specific filesystem on the master.
func (d *DotmeshRPC) Commit(
	r *http.Request, args *CommitArgs,
	result *string,
) error {
	/* Non-admin users are allowed to commit, as a temporary measure
		      until a way of making the frontend tests work without it is found.

	      Please uncomment this code and close https://github.com/dotmesh-io/dotmesh/issues/179
	      when resolved.

			err := ensureAdminUser(r)

			if err != nil {
				return err
			}
	*/

	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	// Insert a command into etcd for the current master to respond to, and
	// wait for a response to be inserted into etcd as well, before firing with
	// that.
	filesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name},
		args.Branch,
	)
	if err != nil {
		return err
	}
	// NB: metadata keys must always start lowercase, because zfs
	user, _, _ := r.BasicAuth()
	meta := metadata{"message": args.Message, "author": user}

	// check that user submitted metadata field names all start with lowercase
	for name, value := range args.Metadata {
		firstCharacter := string(name[0])
		if firstCharacter == strings.ToUpper(firstCharacter) {
			return fmt.Errorf("Metadata field names must start with lowercase characters: %s", name)
		}
		meta[name] = value
	}

	responseChan, err := d.state.globalFsRequest(
		filesystemId,
		&Event{Name: "snapshot",
			Args: &EventArgs{"metadata": meta}},
	)
	if err != nil {
		// meh, maybe REST *would* be nicer
		return err
	}

	// TODO this may never succeed, if the master for it never shows up. maybe
	// this response should have a timeout associated with it.
	e := <-responseChan
	if e.Name == "snapshotted" {
		log.Printf("Snapshotted %s", filesystemId)
		*result = (*e.Args)["SnapshotId"].(string)
	} else {
		return maybeError(e)
	}
	return nil
}

// Rollback a specific filesystem to the specified snapshot_id on the master.
func (d *DotmeshRPC) Rollback(
	r *http.Request,
	args *struct{ Namespace, Name, Branch, SnapshotId string },
	result *bool,
) error {
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	err = requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	// Insert a command into etcd for the current master to respond to, and
	// wait for a response to be inserted into etcd as well, before firing with
	// that.
	filesystemId, err := d.state.registry.MaybeCloneFilesystemId(
		VolumeName{args.Namespace, args.Name},
		args.Branch,
	)
	if err != nil {
		return err
	}
	responseChan, err := d.state.globalFsRequest(
		filesystemId,
		&Event{Name: "rollback",
			Args: &EventArgs{"rollbackTo": args.SnapshotId}},
	)
	if err != nil {
		return err
	}

	// TODO this may never succeed, if the master for it never shows up. maybe
	// this response should have a timeout associated with it.
	e := <-responseChan
	if e.Name == "rolled-back" {
		log.Printf(
			"Rolled back %s/%s@%s to %s",
			args.Namespace,
			args.Name,
			args.Branch,
			args.SnapshotId,
		)
		*result = true
	} else {
		return maybeError(e)
	}
	return nil
}

func maybeError(e *Event) error {
	log.Printf("Unexpected response %s - %s", e.Name, e.Args)
	err, ok := (*e.Args)["err"]
	if ok {
		return err.(error)
	} else {
		return fmt.Errorf("Unexpected response %s - %s", e.Name, e.Args)
	}
}

// Return a list of clone names attributed to a given top-level filesystem name
func (d *DotmeshRPC) Branches(r *http.Request, filesystemName *VolumeName, result *[]string) error {
	err := requireValidVolumeName(*filesystemName)
	if err != nil {
		return err
	}

	filesystemId, err := d.state.registry.IdFromName(*filesystemName)
	if err != nil {
		return err
	}
	filesystems := d.state.registry.ClonesFor(filesystemId)
	names := []string{}
	for name, _ := range filesystems {
		names = append(names, name)
	}
	sort.Strings(names)
	*result = names
	return nil
}

func (d *DotmeshRPC) Branch(
	r *http.Request,
	args *struct{ Namespace, Name, SourceBranch, NewBranchName, SourceCommitId string },
	result *bool,
) error {
	// TODO pass through to a globalFsRequest

	// find the real origin filesystem we're trying to clone from, identified
	// to the user by "volume + sourcebranch", but to us by an underlying
	// filesystem id (could be a clone of a clone)

	// NB: are we special-casing master here? Yes, I think. You'll never be
	// able to delete the master branch because it's equivalent to the
	// topLevelFilesystemId. You could rename it though, I suppose. That's
	// probably fine. We could fix this later by allowing promotions.

	err := requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.SourceBranch)
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.NewBranchName)
	if err != nil {
		return err
	}

	tlf, err := d.state.registry.LookupFilesystem(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}
	var originFilesystemId string

	// find whether branch refers to top-level fs or a clone, by guessing based
	// on name convention. XXX this shouldn't be dealing with "master" and
	// branches
	if args.SourceBranch == DEFAULT_BRANCH {
		originFilesystemId = tlf.MasterBranch.Id
	} else {
		clone, err := d.state.registry.LookupClone(
			tlf.MasterBranch.Id, args.SourceBranch,
		)
		originFilesystemId = clone.FilesystemId
		if err != nil {
			return err
		}
	}
	// target node is responsible for creating registry entry (so that they're
	// as close as possible to eachother), so give it all the info it needs to
	// do that.
	responseChan, err := d.state.globalFsRequest(
		originFilesystemId,
		&Event{Name: "clone",
			Args: &EventArgs{
				"topLevelFilesystemId": tlf.MasterBranch.Id,
				"originFilesystemId":   originFilesystemId,
				"originSnapshotId":     args.SourceCommitId,
				"newBranchName":        args.NewBranchName,
			},
		},
	)
	if err != nil {
		return err
	}

	// TODO this may never succeed, if the master for it never shows up. maybe
	// this response should have a timeout associated with it.
	e := <-responseChan
	if e.Name == "cloned" {
		log.Printf(
			"Cloned %s:%s@%s (%s) to %s", args.Name,
			args.SourceBranch, args.SourceCommitId, originFilesystemId,
		)
		*result = true
	} else {
		return maybeError(e)
	}
	return nil
}

// Return local version information.
func (d *DotmeshRPC) Version(
	r *http.Request, args *struct{}, result *VersionInfo) error {
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	*result = *d.state.versionInfo
	return nil
}

func (d *DotmeshRPC) registerFilesystemBecomeMaster(
	ctx context.Context,
	filesystemNamespace, filesystemName, cloneName, filesystemId string,
	path PathToTopLevelFilesystem,
) error {

	// TODO handle the case where the registry entry exists but the filesystems
	// (fsMachine map) entry doesn't.

	log.Printf("[registerFilesystemBecomeMaster] called: filesystemNamespace=%s, filesystemName=%s, cloneName=%s, filesystemId=%s path=%+v",
		filesystemNamespace, filesystemName, cloneName, filesystemId, path)

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}
	filesystemIds := []string{path.TopLevelFilesystemId}
	for _, c := range path.Clones {
		filesystemIds = append(filesystemIds, c.Clone.FilesystemId)
	}
	for _, f := range filesystemIds {
		// If any filesystemId in the transfer is marked as deleted or
		// cleanupPending, remove that mark. We want to allow it to live again,
		// and we don't want it to be asynchronously deleted!
		deleted, err := isFilesystemDeletedInEtcd(f)
		if err != nil {
			return err
		}
		if deleted {
			_, err = kapi.Delete(
				context.Background(),
				fmt.Sprintf("%s/filesystems/deleted/%s", ETCD_PREFIX, f),
				&client.DeleteOptions{},
			)
			// Key not found means someone deleted it between us checking and
			// us deleting it. Proceed.
			if err != nil && !client.IsKeyNotFound(err) {
				return err
			}
			_, err = kapi.Delete(
				context.Background(),
				fmt.Sprintf("%s/filesystems/cleanupPending/%s", ETCD_PREFIX, f),
				&client.DeleteOptions{},
			)
			if err != nil && !client.IsKeyNotFound(err) {
				return err
			}
		}

		// Only after we've made sure that the fsMachine won't immediately try
		// to delete it (if it's being raised from the dead), ensure there's a
		// filesystem machine for it (and its parents), otherwise it won't
		// process any events. in the case where it already exists, this is a
		// noop.
		log.Printf("[registerFilesystemBecomeMaster] calling initFilesystemMachine for %s", f)
		d.state.initFilesystemMachine(f)
		log.Printf("[registerFilesystemBecomeMaster] done initFilesystemMachine for %s", f)

		_, err = kapi.Get(
			context.Background(),
			fmt.Sprintf(
				"%s/filesystems/masters/%s", ETCD_PREFIX, f,
			),
			nil,
		)
		if err != nil && !client.IsKeyNotFound(err) {
			return err
		}
		if err != nil {
			// TODO: maybe check value, and if it's != me, raise an error?
			// key doesn't already exist
			_, err = kapi.Set(
				context.Background(),
				fmt.Sprintf(
					"%s/filesystems/masters/%s", ETCD_PREFIX, f,
				),
				// i pick -- me!
				// TODO maybe one day pick the node with the most disk space or
				// something
				d.state.myNodeId,
				// only pick myself as current master if no one else has it
				&client.SetOptions{PrevExist: client.PrevNoExist},
			)
			if err != nil {
				return err
			}

			// Immediately update the masters cache because we just wrote
			// to etcd meaning we don't have to wait for a watch
			// this is cconsistent with the code in createFilesystem
			func() {
				d.state.mastersCacheLock.Lock()
				defer d.state.mastersCacheLock.Unlock()
				(*d.state.mastersCache)[filesystemId] = d.state.myNodeId
			}()

		}
	}

	// do this after, in case filesystemId already existed above
	// use path to set up requisite clone metadata

	// set up top level filesystem first, if not exists
	if d.state.registry.Exists(path.TopLevelFilesystemName, "") == "" {
		err = d.state.registry.RegisterFilesystem(
			ctx, path.TopLevelFilesystemName, path.TopLevelFilesystemId,
		)
		if err != nil {
			return err
		}
	}

	// for each clone, set up clone
	for _, c := range path.Clones {
		err = d.state.registry.RegisterClone(c.Name, path.TopLevelFilesystemId, c.Clone)
		if err != nil {
			return err
		}
	}

	log.Printf(
		"[registerFilesystemBecomeMaster] set master and registered fs in registry for %s",
		filesystemId,
	)
	return nil
}

func (d *DotmeshRPC) RegisterFilesystem(
	r *http.Request,
	args *struct {
		Namespace, TopLevelFilesystemName, CloneName, FilesystemId string
		PathToTopLevelFilesystem                                   PathToTopLevelFilesystem
		BecomeMasterIfNotExists                                    bool
	},
	result *bool,
) error {
	log.Printf("[RegisterFilesystem] called with args: %+v", args)

	isAdmin, err := AuthenticatedUserIsNamespaceAdministrator(r.Context(), args.Namespace)
	if err != nil {
		return err
	}

	if !isAdmin {
		return fmt.Errorf("User is not an administrator for namespace %s, so cannot create volumes",
			args.Namespace)
	}

	err = requireValidVolumeName(VolumeName{args.Namespace, args.TopLevelFilesystemName})
	if err != nil {
		return err
	}

	err = requireValidBranchName(args.CloneName)
	if err != nil {
		return err
	}

	if !args.BecomeMasterIfNotExists {
		panic("can't not become master in RegisterFilesystem inter-cluster rpc")
	}
	err = d.registerFilesystemBecomeMaster(
		r.Context(),
		args.Namespace,
		args.TopLevelFilesystemName,
		args.CloneName,
		args.FilesystemId,
		args.PathToTopLevelFilesystem,
	)
	*result = true
	return err
}

func (d *DotmeshRPC) GetTransfer(
	r *http.Request,
	args *string,
	result *TransferPollResult,
) error {
	// Poll the status of a transfer by fetching it from our local cache.
	res, ok := (*d.state.interclusterTransfers)[*args]
	if !ok {
		return fmt.Errorf("No such intercluster transfer %s", *args)
	}
	*result = res
	return nil
}

// Register a transfer from an initiator (the cluster where the user initially
// connected) to a peer (the cluster which will be the target of a push/pull).
func (d *DotmeshRPC) RegisterTransfer(
	r *http.Request,
	args *TransferPollResult,
	result *bool,
) error {
	log.Printf("[RegisterTransfer] called with args: %+v", args)

	// We are the "remote" here. Local name is welcome to be invalid,
	// that's the far end's problem
	err := requireValidVolumeName(VolumeName{args.RemoteNamespace, args.RemoteName})
	if err != nil {
		return err
	}
	err = requireValidBranchName(args.RemoteBranchName)
	if err != nil {
		return err
	}

	serialized, err := json.Marshal(args)
	if err != nil {
		return err
	}
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf(
			"%s/filesystems/transfers/%s", ETCD_PREFIX, args.TransferRequestId,
		),
		string(serialized),
		nil,
	)
	if err != nil {
		return err
	}
	// XXX A transfer should be able to span multiple filesystemIds, really. So
	// tying a transfer to a filesystem id is probably wrong. except, the thing
	// being updated is a specific branch (filesystem id), it's ok if it drags
	// dependent snapshots along with it.
	responseChan, err := d.state.globalFsRequest(args.FilesystemId, &Event{
		Name: "peer-transfer",
		Args: &EventArgs{
			"Transfer": args,
		},
	})
	if err != nil {
		return err
	}

	// Block until the fsmachine is ready to transfer
	log.Printf("[RegisterTransfer:%s] waiting for ack from the fsmachine...", args.FilesystemId)
	e := <-responseChan
	log.Printf("[RegisterTransfer:%s] received ack from the fsmachine: %+v", args.FilesystemId, e)

	if e.Name != "awaiting-transfer" {
		// Something went wrong!
		return fmt.Errorf("Error requesting peer transfer: %+v", e)
	} else {
		return nil
	}

	return nil
}

// Need both push and pull because one cluster will often be behind NAT.
// Transfer will immediately return a transferId which can be queried until
// completion
func (d *DotmeshRPC) Transfer(
	r *http.Request,
	args *TransferRequest,
	result *string,
) error {
	client := NewJsonRpcClient(args.User, args.Peer, args.ApiKey, args.Port)

	log.Printf("[Transfer] starting with %+v", safeArgs(*args))

	// Remote name is welcome to be invalid, that's the far end's problem
	err := requireValidVolumeName(VolumeName{args.LocalNamespace, args.LocalName})
	if err != nil {
		return err
	}
	err = requireValidBranchName(args.LocalBranchName)
	if err != nil {
		return err
	}

	var remoteFilesystemId string
	err = client.CallRemote(r.Context(),
		"DotmeshRPC.Exists", map[string]string{
			"Namespace": args.RemoteNamespace,
			"Name":      args.RemoteName,
			"Branch":    args.RemoteBranchName,
		}, &remoteFilesystemId)
	if err != nil {
		return err
	}

	localFilesystemId := d.state.registry.Exists(
		VolumeName{args.LocalNamespace, args.LocalName}, args.LocalBranchName,
	)

	remoteExists := remoteFilesystemId != ""
	localExists := localFilesystemId != ""

	if !remoteExists && !localExists {
		return fmt.Errorf("Both local and remote filesystems don't exist.")
	}
	if args.Direction == "push" && !localExists {
		return fmt.Errorf("Can't push when local doesn't exist")
	}
	if args.Direction == "pull" && !remoteExists {
		return fmt.Errorf("Can't pull when remote doesn't exist")
	}

	var localPath, remotePath PathToTopLevelFilesystem
	if args.Direction == "push" {
		localPath, err = d.state.registry.deducePathToTopLevelFilesystem(
			VolumeName{args.LocalNamespace, args.LocalName}, args.LocalBranchName,
		)
		if err != nil {
			return fmt.Errorf(
				"Can't deduce path to top level filesystem for %s/%s,%s: %s",
				args.LocalNamespace, args.LocalName, args.LocalBranchName, err,
			)
		}

		// Path is the same on the remote, except with a potentially different name
		remotePath = localPath
		remotePath.TopLevelFilesystemName = VolumeName{args.RemoteNamespace, args.RemoteName}
	} else if args.Direction == "pull" {
		err := client.CallRemote(r.Context(),
			"DotmeshRPC.DeducePathToTopLevelFilesystem", map[string]interface{}{
				"RemoteNamespace":      args.RemoteNamespace,
				"RemoteFilesystemName": args.RemoteName,
				"RemoteCloneName":      args.RemoteBranchName,
			},
			&remotePath,
		)
		if err != nil {
			return fmt.Errorf(
				"Can't deduce path to top level filesystem for %s/%s,%s: %s",
				args.RemoteNamespace, args.RemoteName, args.RemoteBranchName, err,
			)
		}
		// Path is the same locally, except with a potentially different name
		localPath = remotePath
		localPath.TopLevelFilesystemName = VolumeName{args.LocalNamespace, args.LocalName}
	}

	log.Printf("[Transfer] got paths: local=%+v remote=%+v", localPath, remotePath)

	var filesystemId string
	if args.Direction == "push" && !remoteExists {
		// pre-create the remote registry entry and pick a master for it to
		// land on on the remote
		var result bool

		err := client.CallRemote(r.Context(),
			"DotmeshRPC.RegisterFilesystem", map[string]interface{}{
				"Namespace":              args.RemoteNamespace,
				"TopLevelFilesystemName": args.RemoteName,
				"CloneName":              args.RemoteBranchName,
				"FilesystemId":           localFilesystemId,
				// record that you are the master if the fs doesn't exist yet, so
				// that you can receive a push. This should cause an fsMachine to
				// get spawned on this node, listening out for globalFsRequests for
				// this filesystemId on that cluster.
				"BecomeMasterIfNotExists":  true,
				"PathToTopLevelFilesystem": remotePath,
			}, &result)
		if err != nil {
			return err
		}
		filesystemId = localFilesystemId
	} else if args.Direction == "pull" && !localExists {
		// pre-create the local registry entry and pick a master for it to land
		// on locally (me!)
		err = d.registerFilesystemBecomeMaster(
			r.Context(),
			args.LocalNamespace,
			args.LocalName,
			args.LocalBranchName,
			remoteFilesystemId,
			localPath,
		)
		if err != nil {
			return err
		}
		filesystemId = remoteFilesystemId
	} else if remoteExists && localExists && remoteFilesystemId != localFilesystemId {
		return fmt.Errorf(
			"Cannot reconcile filesystems with different ids, remote=%s, local=%s, args=%+v",
			remoteFilesystemId, localFilesystemId, safeArgs(*args),
		)
	} else if remoteExists && localExists && remoteFilesystemId == localFilesystemId {
		filesystemId = localFilesystemId

		// This is an incremental update, not a new filesystem for the writer.
		// Check whether there are uncommitted changes or containers running
		// where the writes are going to happen.

		var cs []DockerContainer
		var dirtyBytes int64

		err = tryUntilSucceedsN(func() error {
			// TODO Add a check that the filesystem hasn't diverged snapshot-wise.

			if args.Direction == "push" {
				// Ask the remote
				var v DotmeshVolume
				err := client.CallRemote(r.Context(), "DotmeshRPC.Get", filesystemId, &v)
				if err != nil {
					return err
				}
				log.Printf("[TransferIt] for %s, got dotmesh volume: %s", filesystemId, v)
				dirtyBytes = v.DirtyBytes
				log.Printf("[TransferIt] got %d dirty bytes for %s from peer", dirtyBytes, filesystemId)

				err = client.CallRemote(r.Context(), "DotmeshRPC.ContainersById", filesystemId, &cs)
				if err != nil {
					return err
				}
				log.Printf("[TransferIt] got %+v remote containers for %s from peer", cs, filesystemId)

			} else if args.Direction == "pull" {
				// Consult ourselves
				v, err := d.state.getOne(r.Context(), filesystemId)
				if err != nil {
					return err
				}
				dirtyBytes = v.DirtyBytes
				log.Printf("[TransferIt] got %d dirty bytes for %s from local", dirtyBytes, filesystemId)

				d.state.globalContainerCacheLock.Lock()
				defer d.state.globalContainerCacheLock.Unlock()
				c, _ := (*d.state.globalContainerCache)[filesystemId]
				cs = c.Containers
			}

			if dirtyBytes > 0 {
				// TODO backoff and retry above
				return fmt.Errorf(
					"Aborting because there are %.2f MiB of uncommitted changes on volume "+
						"where data would be written. Use 'dm reset' to roll back.",
					float64(dirtyBytes)/(1024*1024),
				)
			}

			if len(cs) > 0 {
				containersRunning := []string{}
				for _, c := range cs {
					containersRunning = append(containersRunning, string(c.Name))
				}
				return fmt.Errorf(
					"Aborting because there are active containers running on "+
						"volume where data would be written: %s. Stop the containers.",
					strings.Join(containersRunning, ", "),
				)
			}
			return nil
		}, "checking for dirty data and running containers", 2)
		if err != nil {
			return err
		}

	} else {
		return fmt.Errorf(
			"Unexpected combination of factors: "+
				"remoteExists: %t, localExists: %t, "+
				"remoteFilesystemId: %s, localFilesystemId: %s",
			remoteExists, localExists, remoteFilesystemId, localFilesystemId,
		)
	}

	// Now run globalFsRequest, returning the request id, to make the master of
	// a (possibly nonexisting) filesystem start pulling or pushing it, and
	// make it update status as it goes in a new pollable "transfers" object in
	// etcd.

	responseChan, requestId, err := d.state.globalFsRequestId(
		filesystemId,
		&Event{Name: "transfer",
			Args: &EventArgs{
				"Transfer": args,
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
		log.Printf("finished transfer of %+v, %+v", args, e)
	}()

	*result = requestId
	return nil
}

func safeArgs(t TransferRequest) TransferRequest {
	t.ApiKey = "<redacted>"
	return t
}

func (a ByAddress) Len() int      { return len(a) }
func (a ByAddress) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByAddress) Less(i, j int) bool {
	if len(a[i].Addresses) == 0 || len(a[j].Addresses) == 0 {
		return false
	}
	return a[i].Addresses[0] < a[j].Addresses[0]
}

// Return data showing all volumes, their clones, along with information about
// them such as the current state of their state machines on each server, etc.
//
// TODO should this function be the same as List?
func (d *DotmeshRPC) AllDotsAndBranches(
	r *http.Request,
	args *struct{},
	result *VolumesAndBranches,
) error {
	quietLogger("[AllDotsAndBranches] starting...")

	vac := VolumesAndBranches{}

	d.state.serverAddressesCacheLock.Lock()
	for server, addresses := range *d.state.serverAddressesCache {
		vac.Servers = append(vac.Servers, Server{
			Id: server, Addresses: strings.Split(addresses, ","),
		})
	}
	d.state.serverAddressesCacheLock.Unlock()
	sort.Sort(ByAddress(vac.Servers))

	filesystemNames := d.state.registry.Filesystems()
	for _, fsName := range filesystemNames {
		tlfId, err := d.state.registry.IdFromName(fsName)
		if err != nil {
			return err
		}
		// XXX: crappyTlf is crappy because it contains an incomplete
		// TopLevelFilesystem object (see UpdateFilesystemFromEtcd). The only
		// thing we use it for here is the owner and collaborator data, and we
		// construct a new TopLevelFilesystem for ourselves. Probably the
		// following logic should be put somewhere inside the registry...
		crappyTlf, err := d.state.registry.LookupFilesystem(fsName)
		if err != nil {
			return err
		}
		tlf := TopLevelFilesystem{}
		/*
			MasterBranch DotmeshVolume
			CloneVolumes   []DotmeshVolume
			Owner          User
			Collaborators  []User
		*/
		v, err := d.state.getOne(r.Context(), tlfId)
		// Just skip this in the result list if the context (eg authenticated
		// user) doesn't have permission to read it.
		if err != nil {
			switch err := err.(type) {
			default:
				log.Printf("[AllDotsAndBranches] ERROR in getOne(%v): %v, continuing...", tlfId, err)
				continue
			case PermissionDenied:
				continue
			}
		}
		tlf.MasterBranch = v
		// now add clones to tlf
		clones := d.state.registry.ClonesFor(tlfId)
		cloneNames := []string{}
		for c, _ := range clones {
			cloneNames = append(cloneNames, c)
		}
		sort.Strings(cloneNames)
		for _, cloneName := range cloneNames {
			clone := clones[cloneName]
			c, err := d.state.getOne(r.Context(), clone.FilesystemId)
			if err != nil {
				return err
			}
			tlf.OtherBranches = append(tlf.OtherBranches, c)
		}
		tlf.Owner = crappyTlf.Owner
		tlf.Collaborators = crappyTlf.Collaborators
		vac.Dots = append(vac.Dots, tlf)
	}
	*result = vac
	quietLogger("[AllDotsAndBranches] finished!")
	return nil
}

func (d *DotmeshRPC) AddCollaborator(
	r *http.Request,
	args *struct {
		MasterBranchID string
		Collaborator   string
	},
	result *bool,
) error {
	// check authenticated user is owner of volume.
	crappyTlf, clone, err := d.state.registry.LookupFilesystemById(args.MasterBranchID)
	if err != nil {
		return err
	}
	if clone != "" {
		return fmt.Errorf(
			"Please add collaborators to the master branch of the dot",
		)
	}
	authorized, err := crappyTlf.AuthorizeOwner(r.Context())
	if err != nil {
		return err
	}
	if !authorized {
		return fmt.Errorf(
			"Not owner. Please ask the owner to add the collaborator.",
		)
	}
	// add collaborator in registry, re-save.
	potentialCollaborator, err := GetUserByName(args.Collaborator)
	if err != nil {
		return err
	}
	newCollaborators := append(crappyTlf.Collaborators, safeUser(potentialCollaborator))
	err = d.state.registry.UpdateCollaborators(r.Context(), crappyTlf, newCollaborators)
	if err != nil {
		return err
	}
	*result = true
	return nil
}

func (d *DotmeshRPC) RemoveCollaborator(
	r *http.Request,
	args *struct {
		MasterBranchID string
		Collaborator   string
	},
	result *bool,
) error {
	// check authenticated user is owner of volume.
	crappyTlf, clone, err := d.state.registry.LookupFilesystemById(args.MasterBranchID)
	if err != nil {
		return err
	}
	if clone != "" {
		return fmt.Errorf(
			"Please remove collaborators from the master branch of the dot",
		)
	}
	authorized, err := crappyTlf.AuthorizeOwner(r.Context())
	if err != nil {
		return err
	}
	if !authorized {
		return fmt.Errorf(
			"Not owner. Please ask the owner to remove the collaborator.",
		)
	}

	authenticatedUserId := r.Context().Value("authenticated-user-id").(string)
	authenticatedUser, err := GetUserById(authenticatedUserId)

	if err != nil {
		return err
	}

	if authenticatedUser.Name == args.Collaborator {
		return fmt.Errorf(
			"You cannot remove yourself as a collaborator from a dot.",
		)
	}

	collaboratorIndex := -1

	for i, collaborator := range crappyTlf.Collaborators {
		if collaborator.Name == args.Collaborator {
			collaboratorIndex = i
		}
	}

	if collaboratorIndex == -1 {
		return fmt.Errorf(
			"%s is not a collaborator on this dot so cannot remove.",
			args.Collaborator,
		)
	}

	// remove collaborator in registry, re-save.
	newCollaborators := append(crappyTlf.Collaborators[:collaboratorIndex], crappyTlf.Collaborators[collaboratorIndex+1:]...)

	err = d.state.registry.UpdateCollaborators(r.Context(), crappyTlf, newCollaborators)
	if err != nil {
		return err
	}
	*result = true
	return nil
}

func (d *DotmeshRPC) DeducePathToTopLevelFilesystem(
	r *http.Request,
	args *struct {
		RemoteNamespace      string
		RemoteFilesystemName string
		RemoteCloneName      string
	},
	result *PathToTopLevelFilesystem,
) error {
	log.Printf("[DeducePathToTopLevelFilesystem] called with args: %+v", args)

	err := requireValidVolumeName(VolumeName{args.RemoteNamespace, args.RemoteFilesystemName})
	if err != nil {
		return err
	}
	err = requireValidBranchName(args.RemoteCloneName)
	if err != nil {
		return err
	}

	res, err := d.state.registry.deducePathToTopLevelFilesystem(
		VolumeName{args.RemoteNamespace, args.RemoteFilesystemName}, args.RemoteCloneName,
	)
	if err != nil {
		return err
	}
	*result = res
	log.Printf("[DeducePathToTopLevelFilesystem] succeeded: args %+v -> result %+v", args, res)
	return nil
}

func (d *DotmeshRPC) PredictSize(
	r *http.Request,
	args *struct {
		FromFilesystemId string
		FromSnapshotId   string
		ToFilesystemId   string
		ToSnapshotId     string
	},
	result *int64,
) error {
	log.Printf("[PredictSize] got args %+v", args)
	size, err := predictSize(
		args.FromFilesystemId, args.FromSnapshotId, args.ToFilesystemId, args.ToSnapshotId,
	)
	if err != nil {
		return err
	}
	*result = size
	return nil
}

func checkNotInUse(d *DotmeshRPC, fsid string, origins map[string]string) error {
	containersInUse := func() int {
		d.state.globalContainerCacheLock.Lock()
		defer d.state.globalContainerCacheLock.Unlock()
		containerInfo, ok := (*d.state.globalContainerCache)[fsid]
		if !ok {
			return 0
		}
		return len(containerInfo.Containers)
	}()
	if containersInUse > 0 {
		return fmt.Errorf("We cannot delete the volume %s when %d containers are still using it", fsid, containersInUse)
	}
	for child, parent := range origins {
		if parent == fsid {
			err := checkNotInUse(d, child, origins)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func sortFilesystemsInDeletionOrder(in []string, rootId string, origins map[string]string) []string {
	// Recursively zap any children
	for child, parent := range origins {
		if parent == rootId {
			in = sortFilesystemsInDeletionOrder(in, child, origins)
		}
	}
	// Then zap the root
	in = append(in, rootId)
	return in
}

func (d *DotmeshRPC) Delete(
	r *http.Request,
	args *VolumeName,
	result *bool,
) error {
	*result = false

	err := requireValidVolumeName(*args)
	if err != nil {
		return err
	}

	user, err := GetUserById(r.Context().Value("authenticated-user-id").(string))

	// Look up the top-level filesystem. This will error if the
	// filesystem name isn't registered.
	filesystem, err := d.state.registry.LookupFilesystem(*args)
	if err != nil {
		return err
	}

	authorized, err := filesystem.AuthorizeOwner(r.Context())
	if err != nil {
		return err
	}
	if !authorized {
		return fmt.Errorf(
			"You are not the owner of volume %s/%s. Only the owner can delete it.",
			args.Namespace, args.Name,
		)

	}

	// Find the list of all clones of the filesystem, as we need to delete each independently.
	filesystems := d.state.registry.ClonesFor(filesystem.MasterBranch.Id)

	// We can't destroy a filesystem that's an origin for another
	// filesystem, so let's topologically sort them and destroy them leaves-first.

	// Analyse the list of filesystems, putting it into a more useful form for our purposes
	origins := make(map[string]string)
	names := make(map[string]string)
	for name, fs := range filesystems {
		// Record the origin
		origins[fs.FilesystemId] = fs.Origin.FilesystemId
		// Record the name
		names[fs.FilesystemId] = name
	}

	// FUTURE WORK: If we ever need to delete just some clones, we
	// can do so by picking a different rootId here. See
	// https://github.com/dotmesh-io/dotmesh/issues/58
	rootId := filesystem.MasterBranch.Id

	// Check all clones are not in use. This is no guarantee one won't
	// come into use while we're processing the deletion, but it's nice
	// for the user to try and check first.

	err = checkNotInUse(d, rootId, origins)
	if err != nil {
		return err
	}

	filesystemsInOrder := make([]string, 0)
	filesystemsInOrder = sortFilesystemsInDeletionOrder(filesystemsInOrder, rootId, origins)

	// What if we are interrupted during this loop?

	// Because we delete from the leaves up, we SHOULD be OK: the
	// system may end up in a state where some clones are gone, but
	// the top-level filesystem remains and a new deletion on that
	// should pick up where we left off.  I don't know how to easily
	// test that with the current test harness, however, so here's
	// hoping I'm right.
	for _, fsid := range filesystemsInOrder {
		// At this point, check the filesystem has no containers
		// using it and error if so, for usability. This does not mean the
		// filesystem is unused from here onwards, as it could come into
		// use at any point.

		// This will error if the filesystem is already marked as
		// deleted; it shouldn't be in the metadata if it was, so
		// hopefully that will never happen.
		if filesystem.MasterBranch.Id == fsid {
			// master clone, so record the name to delete and no clone registry entry to delete
			err = d.state.markFilesystemAsDeletedInEtcd(fsid, user.Name, *args, "", "")
		} else {
			// Not the master clone, so don't record a name to delete, but do record a clone name for deletion
			err = d.state.markFilesystemAsDeletedInEtcd(
				fsid, user.Name, VolumeName{},
				filesystem.MasterBranch.Id, names[fsid])
		}
		if err != nil {
			return err
		}

		// Block until the filesystem is gone locally (it may still be
		// dying on other nodes in the cluster, but it's too costly to
		// track that for the gains it gives us)
		d.state.waitForFilesystemDeath(fsid)

		// As we only block for completion locally, there IS a chance
		// that the deletions will happen in the wrong order on other
		// nodes in the cluster.  This may mean that some of them fail
		// with an error, because their origins still exist.  However,
		// hopefully, the discovery-triggers-redeletion code will cause
		// them to eventually be deleted.
	}

	// If we're deleting the entire filesystem rather than just a
	// clone, we need to unregister it.

	// At this point, we have an inconsistent system state: the clone
	// filesystems are marked for deletion, but their name is still
	// registered in the registry. If we crash here, the name is taken
	// by a nonexistant filesystem.

	// This, however, is then recovered from by the
	// cleanupDeletedFilesystems function, which is invoked
	// periodically.

	if rootId == filesystem.MasterBranch.Id {
		err = d.state.registry.UnregisterFilesystem(*args)
		if err != nil {
			return err
		}
	}

	*result = true
	return nil
}

func handleBooleanFlag(flag *bool, value string, oldValue *string) {
	if *flag {
		*oldValue = "true"
	} else {
		*oldValue = "false"
	}

	if value == "true" {
		*flag = true
	} else {
		*flag = false
	}
}

func (d *DotmeshRPC) SetDebugFlag(
	r *http.Request,
	args *struct {
		FlagName  string
		FlagValue string
	},
	result *string,
) error {
	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	log.Printf("DEBUG FLAG: %s -> %s (was %s)", args.FlagName, args.FlagValue, *result)

	switch args.FlagName {
	case "PartialFailCreateFilesystem":
		handleBooleanFlag(&d.state.debugPartialFailCreateFilesystem, args.FlagValue, result)
	case "ForceStateMachineToDiscovering":
		filesystemId := args.FlagValue
		responseChan, err := d.state.globalFsRequest(
			filesystemId,
			&Event{Name: "deliberately-unhandled-event-for-test-purposes"},
		)
		if err != nil {
			return err
		}
		e := <-responseChan
		log.Printf("[SetDebugFlag] deliberately unhandled event replied with %#v", e)
		*result = ""
	default:
		*result = ""
		return fmt.Errorf("Unknown debug flag %s", args.FlagName)
	}

	log.Printf("DEBUG FLAG: %s <- %s (was %s)", args.FlagName, args.FlagValue, *result)
	return nil
}

func (d *DotmeshRPC) DumpInternalState(
	r *http.Request,
	filter *string,
	result *map[string]string,
) error {
	err := ensureAdminUser(r)
	if err != nil {
		return err
	}

	// Set up a goroutine gathering data into *result via resultChan
	*result = map[string]string{}

	resultChan := make(chan ([]string))
	doneChan := make(chan bool)

	go func() {
		for pair := range resultChan {
			key := pair[0]
			val := pair[1]

			// empty string is prefix of everything
			if filter == nil || strings.HasPrefix(key, *filter) {
				(*result)[key] = val
			}
		}
		doneChan <- true
	}()

	// Gather data, using the channel, putting anything that might block in a goroutine
	s := d.state

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"filesystems.STARTED", "yes"}
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()

		for id, fs := range *(s.filesystems) {
			resultChan <- []string{fmt.Sprintf("filesystems.%s.STARTED", id), "yes"}
			fs.snapshotsLock.Lock()
			defer fs.snapshotsLock.Unlock()

			resultChan <- []string{fmt.Sprintf("filesystems.%s.id", id), fs.filesystemId}
			if fs.filesystem != nil {
				resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.id", id), fs.filesystem.id}
				resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.exists", id), fmt.Sprintf("%t", fs.filesystem.exists)}
				resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.mounted", id), fmt.Sprintf("%t", fs.filesystem.mounted)}
				resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.origin", id), fmt.Sprintf("%s@%s", fs.filesystem.origin.FilesystemId, fs.filesystem.origin.SnapshotId)}
				for idx, snapshot := range fs.filesystem.snapshots {
					resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.snapshots[%d].id", id, idx), snapshot.Id}
					for key, val := range *(snapshot.Metadata) {
						resultChan <- []string{fmt.Sprintf("filesystems.%s.filesystem.snapshots[%d].metadata.%s", id, idx, key), val}
					}
				}
			}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.currentState", id), fs.currentState}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.status", id), fs.status}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.lastTransitionTimestamp", id), fmt.Sprintf("%d", fs.lastTransitionTimestamp)}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.lastTransferRequest", id), toJsonString(fs.lastTransferRequest)}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.lastTransferRequestId", id), fs.lastTransferRequestId}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.dirtyDelta", id), fmt.Sprintf("%d", fs.dirtyDelta)}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.sizeBytes", id), fmt.Sprintf("%d", fs.sizeBytes)}
			if fs.lastPollResult != nil {
				resultChan <- []string{fmt.Sprintf("filesystems.%s.lastPollResult", id), toJsonString(*fs.lastPollResult)}
			}
			if fs.handoffRequest != nil {
				resultChan <- []string{fmt.Sprintf("filesystems.%s.handoffRequest", id), toJsonString(*fs.handoffRequest)}
			}
			resultChan <- []string{fmt.Sprintf("filesystems.%s.DONE", id), "yes"}
		}
		resultChan <- []string{"filesystems.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"mastersCache.STARTED", "yes"}
		s.mastersCacheLock.Lock()
		defer s.mastersCacheLock.Unlock()
		for fsId, server := range *(s.mastersCache) {
			resultChan <- []string{fmt.Sprintf("mastersCache.%s", fsId), server}
		}
		resultChan <- []string{"mastersCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"serverAddressesCache.STARTED", "yes"}
		s.serverAddressesCacheLock.Lock()
		defer s.serverAddressesCacheLock.Unlock()
		for serverId, addr := range *(s.serverAddressesCache) {
			resultChan <- []string{fmt.Sprintf("serverAddressesCache.%s", serverId), addr}
		}
		resultChan <- []string{"serverAddressesCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"globalSnapshotCache.STARTED", "yes"}
		s.globalSnapshotCacheLock.Lock()
		defer s.globalSnapshotCacheLock.Unlock()
		for serverId, d := range *(s.globalSnapshotCache) {
			for fsId, snapshots := range d {
				for idx, snapshot := range snapshots {
					resultChan <- []string{fmt.Sprintf("globalSnapshotCache.%s.%s.snapshots[%d].id", serverId, fsId, idx), snapshot.Id}
					for key, val := range *(snapshot.Metadata) {
						resultChan <- []string{fmt.Sprintf("globalSnapshotCache.%s.%s.snapshots[%d].metadata.%s", serverId, fsId, idx, key), val}
					}
				}
			}
		}
		resultChan <- []string{"globalSnapshotCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"globalStateCache.STARTED", "yes"}
		s.globalStateCacheLock.Lock()
		defer s.globalStateCacheLock.Unlock()
		for serverId, d := range *(s.globalStateCache) {
			for fsId, states := range d {
				for key, val := range states {
					resultChan <- []string{fmt.Sprintf("globalStateCache.%s.%s.%s", serverId, fsId, key), val}
				}
			}
		}
		resultChan <- []string{"globalStateCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"globalContainerCache.STARTED", "yes"}
		s.globalContainerCacheLock.Lock()
		defer s.globalContainerCacheLock.Unlock()
		for fsId, ci := range *(s.globalContainerCache) {
			resultChan <- []string{fmt.Sprintf("globalContainerCache.%s", fsId), toJsonString(ci)}
		}
		resultChan <- []string{"globalContainerCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"globalDirtyCache.STARTED", "yes"}
		s.globalDirtyCacheLock.Lock()
		defer s.globalDirtyCacheLock.Unlock()
		for fsId, di := range *(s.globalDirtyCache) {
			resultChan <- []string{fmt.Sprintf("globalDirtyCache.%s", fsId), toJsonString(di)}
		}
		resultChan <- []string{"globalDirtyCache.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"interclusterTransfers.STARTED", "yes"}
		s.interclusterTransfersLock.Lock()
		defer s.interclusterTransfersLock.Unlock()
		for txId, tpr := range *(s.interclusterTransfers) {
			resultChan <- []string{fmt.Sprintf("interclusterTransfers.%s", txId), toJsonString(tpr)}
		}
		resultChan <- []string{"interclusterTransfers.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"registry.TopLevelFilesystems.STARTED", "yes"}
		s.registry.TopLevelFilesystemsLock.Lock()
		defer s.registry.TopLevelFilesystemsLock.Unlock()
		for vn, tlf := range s.registry.TopLevelFilesystems {
			resultChan <- []string{fmt.Sprintf("registry.TopLevelFilesystems.%s/%s.MasterBranch.id", vn.Namespace, vn.Name), tlf.MasterBranch.Id}
			// FIXME: MasterBranch is a DotmeshVolume, with many other fields we could display.
			for idx, ob := range tlf.OtherBranches {
				resultChan <- []string{fmt.Sprintf("registry.TopLevelFilesystems.%s/%s.OtherBranches[%d].id", vn.Namespace, vn.Name, idx), ob.Id}
				resultChan <- []string{fmt.Sprintf("registry.TopLevelFilesystems.%s/%s.OtherBranches[%d].name", vn.Namespace, vn.Name, idx), ob.Branch}
				// FIXME: ob is a DotmeshVolume, with many other fields we could display.
			}
			resultChan <- []string{fmt.Sprintf("registry.TopLevelFilesystems.%s/%s.Owner", vn.Namespace, vn.Name), toJsonString(tlf.Owner)}
			for idx, c := range tlf.Collaborators {
				resultChan <- []string{fmt.Sprintf("registry.TopLevelFilesystems.%s/%s.Collaborators[%d]", vn.Namespace, vn.Name, idx), toJsonString(c)}
			}
		}
		resultChan <- []string{"registry.TopLevelFilesystems.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"registry.Clones.STARTED", "yes"}
		s.registry.ClonesLock.Lock()
		defer s.registry.ClonesLock.Unlock()
		for fsId, c := range s.registry.Clones {
			for branchName, clone := range c {
				resultChan <- []string{fmt.Sprintf("registry.Clones.%s.%s.id", fsId, branchName), clone.FilesystemId}
			}
		}
		resultChan <- []string{"registry.Clones.DONE", "yes"}
	}()

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"etcdWait.STARTED", "yes"}
		s.etcdWaitTimestampLock.Lock()
		defer s.etcdWaitTimestampLock.Unlock()
		resultChan <- []string{"etcdWait.Timestamp", fmt.Sprintf("%d", s.etcdWaitTimestamp)}
		resultChan <- []string{"etcdWait.State", s.etcdWaitState}
		resultChan <- []string{"etcdWait.DONE", "yes"}
	}()

	resultChan <- []string{"myNodeId", s.myNodeId}
	resultChan <- []string{"versionInfo", toJsonString(s.versionInfo)}

	go func() {
		defer recover() // Don't kill the entire server if resultChan is closed because we took too long
		resultChan <- []string{"goroutines.STARTED", "yes"}

		numProfiles := 1
		profiles := make([]runtime.StackRecord, numProfiles)

		for {
			numProfiles, ok := runtime.GoroutineProfile(profiles)
			if ok {
				break
			} else {
				// Grow the profiles array and try again
				profiles = make([]runtime.StackRecord, numProfiles)
				continue
			}
		}

		for grIdx, sr := range profiles {
			stack := sr.Stack()
			frames := runtime.CallersFrames(stack)
			idx := 0
			for {
				frame, more := frames.Next()
				// To keep this example's output stable
				// even if there are changes in the testing package,
				// stop unwinding when we leave package runtime.
				resultChan <- []string{
					fmt.Sprintf("goroutines.%d.stack[%03d]", grIdx, idx),
					fmt.Sprintf("%s (%s:%d)", frame.Function, frame.File, frame.Line),
				}

				idx = idx + 1
				if !more {
					break
				}
			}
		}
		resultChan <- []string{"goroutines.DONE", "yes"}
	}()

	// Give all goroutines a second at most to run
	time.Sleep(1 * time.Second)

	// Shut down the collector goroutine
	close(resultChan)

	// Await its confirmation so we can take ownership of result
	<-doneChan

	// Return the result
	log.Printf("[DumpInternalState] finished")
	return nil
}

func (d *DotmeshRPC) DumpEtcd(
	r *http.Request,
	args *struct {
		Prefix string
	},
	result *string,
) error {
	err := ensureAdminUser(r)
	if err != nil {
		return err
	}

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	node, err := kapi.Get(context.Background(),
		fmt.Sprintf("%s/%s", ETCD_PREFIX, args.Prefix),
		&client.GetOptions{Recursive: true, Sort: false, Quorum: false},
	)
	if err != nil {
		return err
	}

	resultBytes, err := json.Marshal(node)
	if err != nil {
		return err
	}

	*result = string(resultBytes)

	return nil
}

func find(node *client.Node, path []string) *client.Node {
	for _, node := range node.Nodes {
		shrapnel := strings.Split(node.Key, "/")
		last := shrapnel[len(shrapnel)-1]
		if last == path[0] {
			if len(path) == 1 {
				return node
			} else {
				return find(node, path[1:])
			}
		}
	}
	log.Printf("Couldn't find %s in %+v -> %+v", path[0], node, node.Nodes)
	return nil
}

func (d *DotmeshRPC) RestoreEtcd(
	r *http.Request,
	args *struct {
		Prefix string
		Dump   string
	},
	result *bool,
) error {
	// We dangerously, blindly trust the contents of the dump.
	err := ensureAdminUser(r)
	if err != nil {
		return err
	}

	if !(args.Prefix == "" || args.Prefix == "/") {
		return fmt.Errorf("Don't know how to restore a dump from a non-root key.")
	}

	// What do we restore?
	// * users (except admin user)
	// * registry

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}
	response := client.Response{}
	err = json.Unmarshal([]byte(args.Dump), &response)
	if err != nil {
		return err
	}

	// Before destroying the registry, back it up
	node, err := kapi.Get(context.Background(),
		fmt.Sprintf("%s/registry", ETCD_PREFIX),
		&client.GetOptions{Recursive: true, Sort: false, Quorum: false},
	)
	if err != nil && !client.IsKeyNotFound(err) {
		return err
	}

	// Don't try to back up a key that doesn't exist.
	if err == nil {
		resultBytes, err := json.Marshal(node)
		if err != nil {
			return err
		}

		// XXX: might run into size limits on keys one day.
		_, err = kapi.Set(context.Background(),
			fmt.Sprintf("%s/registry-backup-%d", ETCD_PREFIX, time.Now().Unix()),
			string(resultBytes),
			&client.SetOptions{},
		)
	}

	_, err = kapi.Delete(context.Background(),
		fmt.Sprintf("%s/registry", ETCD_PREFIX),
		&client.DeleteOptions{Recursive: true, Dir: true},
	)

	if err != nil && !client.IsKeyNotFound(err) {
		// It's OK if it didn't exist.
		return err
	}

	oneLevelNodesToClobber := []*client.Node{
		find(response.Node, []string{"users"}),
		find(response.Node, []string{"filesystems", "masters"}),
	}

	twoLevelNodesToClobber := []*client.Node{
		find(response.Node, []string{"registry", "filesystems"}),
		find(response.Node, []string{"registry", "clones"}),
	}

	setEtcdKey := func(n client.Node) error {
		if n.Key == fmt.Sprintf("%s/users/%s", ETCD_PREFIX, ADMIN_USER_UUID) {
			// don't restore the admin user
			return nil
		}
		_, err = kapi.Set(
			context.Background(),
			n.Key,
			n.Value,
			// Only set values that don't already exist. This is so that
			// restoring from a backup of the same cluster will restore masters
			// records while it won't clobber masters records for other
			// clusters (which e.g. are being pushed to as a backup).  Note
			// that because we delete the entire registry, registry entries
			// _will_ get updated (e.g. adding collaborators to a dot).
			&client.SetOptions{
				PrevExist: client.PrevNoExist,
			},
		)
		// Avoid clobbering existing keys, but don't fail.
		if !strings.Contains(fmt.Sprintf("%s", err), "Key already exists") {
			return err
		}
		return nil
	}

	for _, node := range oneLevelNodesToClobber {
		if node == nil {
			// no users!?
			continue
		}
		for _, n := range node.Nodes {
			err = setEtcdKey(*n)
			if err != nil {
				return err
			}
		}
	}
	for _, node := range twoLevelNodesToClobber {
		if node == nil {
			// no filesystems or clones is valid case
			continue
		}
		for _, n := range node.Nodes {
			for _, nn := range n.Nodes {
				err = setEtcdKey(*nn)
				if err != nil {
					return err
				}
			}
		}
	}

	// Don't restore:
	// * masters
	// * request/response data
	// * admin user
	// * anything else :)

	// In-memory state will be correctly updated by receiving from the etcd
	// watch.

	*result = true
	return nil
}

func (d *DotmeshRPC) GetReplicationLatencyForBranch(
	r *http.Request,
	args *struct {
		Namespace, Name, Branch string
	},
	result *map[string][]string, // Map from server name to list of commits it's missing
) error {
	log.Printf("[GetReplicationLatencyForBranch] being called with: %+v", args)

	err := ensureAdminUser(r)

	if err != nil {
		return err
	}

	err = requireValidVolumeName(VolumeName{args.Namespace, args.Name})
	if err != nil {
		return err
	}
	err = requireValidBranchName(args.Branch)
	if err != nil {
		return err
	}

	fs, err := d.state.registry.MaybeCloneFilesystemId(VolumeName{Namespace: args.Namespace, Name: args.Name}, args.Branch)
	if err != nil {
		return err
	}

	err, res := d.state.GetReplicationLatency(fs)
	if err != nil {
		return err
	}

	*result = res

	return nil
}

func (d *DotmeshRPC) ForceBranchMasterById(
	r *http.Request,
	args *struct {
		FilesystemId string
		Master       string
	},
	result *bool,
) error {
	log.Printf("[ForceBranchMasterById] being called with: %+v", args)

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	newMaster := args.Master
	if newMaster == "" {
		// Default is THIS node
		newMaster = d.state.myNodeId
	}

	key := fmt.Sprintf(
		"%s/filesystems/masters/%s", ETCD_PREFIX, args.FilesystemId,
	)

	log.Printf("[ForceBranchMasterById] settings %s to %s", key, newMaster)

	_, err = kapi.Set(
		context.Background(),
		key,
		newMaster,
		&client.SetOptions{},
	)

	if err != nil {
		return err
	}

	*result = true
	return nil
}
