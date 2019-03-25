package client

/*
A local configuration system for storing "clusters" (remote API targets for the
CLI) and authentication tokens for accessing them.

A user can log in to zero or more clusters. It's important that they're able to
be logged in to more than one cluster at a time, for example to be able to
"push" from one to another.

$ dm remote add origin luke@192.168.1.12
Logging in to dotmesh cluster at 192.168.1.12...
API key: deadbeefcafebabe
Checking login... confirmed!
Login saved in local configuration. Active cluster now origin.

$ dm remote -v
origin     luke@192.168.1.12

How this diverges from git: the CLI itself is logged into one global set of
"remotes", *not* per-repo. This is because there are no local repos. Does this
matter?
*/

import (
	"encoding/json"
	"fmt"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
)

type Remote interface {
	DefaultNamespace() string
	DefaultRemoteVolumeFor(string, string) (string, string, bool)
	SetDefaultRemoteVolumeFor(string, string, string, string)
}

type S3Remote struct {
	KeyID                string
	SecretKey            string
	Endpoint             string
	DefaultRemoteVolumes map[string]map[string]S3VolumeName
}

type DMRemote struct {
	User                 string
	Hostname             string
	Port                 int `json:",omitempty"`
	ApiKey               string
	CurrentVolume        string
	CurrentBranches      map[string]string
	DefaultRemoteVolumes map[string]map[string]types.VolumeName
}

func (remote DMRemote) DefaultNamespace() string {
	return remote.User
}

func (remote S3Remote) DefaultNamespace() string {
	return ""
}

// TODO is there a less hacky way of doing this? hate the duplication, but otherwise you need to cast all over the place
func (remote *DMRemote) SetDefaultRemoteVolumeFor(localNamespace, localVolume, remoteNamespace, remoteVolume string) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]types.VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]types.VolumeName{}
	}
	remote.DefaultRemoteVolumes[localNamespace][localVolume] = types.VolumeName{remoteNamespace, remoteVolume}
}

func (remote *DMRemote) DefaultRemoteVolumeFor(localNamespace, localVolume string) (string, string, bool) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]types.VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]types.VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	if ok {
		return volName.Namespace, volName.Name, ok
	}
	return "", "", false
}

func (remote *S3Remote) SetDefaultRemoteVolumeFor(localNamespace, localVolume, remoteNamespace, remoteVolume string) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]S3VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]S3VolumeName{}
	}
	remote.DefaultRemoteVolumes[localNamespace][localVolume] = S3VolumeName{
		Namespace: remoteNamespace,
		Name:      remoteVolume,
	}
}

func (remote *S3Remote) SetPrefixesFor(localNamespace, localVolume string, prefixes []string) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]S3VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]S3VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	if ok {
		volName.Prefixes = prefixes
		remote.DefaultRemoteVolumes[localNamespace][localVolume] = volName
	} // todo should we handle setting prefixes before setting the remote namespace? probably not important
}

func (remote *S3Remote) PrefixesFor(localNamespace, localVolume string) ([]string, bool) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]S3VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]S3VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	if ok {
		return volName.Prefixes, ok
	}
	return nil, false
}

func (remote *S3Remote) DefaultRemoteVolumeFor(localNamespace, localVolume string) (string, string, bool) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]S3VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]S3VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	if ok {
		return volName.Namespace, volName.Name, ok
	}
	return "", "", false
}

func (remote DMRemote) String() string {
	v := reflect.ValueOf(remote)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type Configuration struct {
	CurrentRemote string
	DMRemotes     map[string]*DMRemote `json:"Remotes"`
	S3Remotes     map[string]*S3Remote
	lock          sync.Mutex
	configPath    string
}

func NewConfiguration(configPath string) (*Configuration, error) {
	c := &Configuration{
		configPath: configPath,
		DMRemotes:  make(map[string]*DMRemote),
		S3Remotes:  make(map[string]*S3Remote),
	}
	if err := c.Load(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Configuration) Load() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, err := os.Stat(c.configPath); os.IsNotExist(err) {
		// Just return with defaults if file does not exist
		return nil
	}
	serialized, err := ioutil.ReadFile(c.configPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(serialized, &c); err != nil {
		return err
	}
	return nil
}

func (c *Configuration) Save() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.save()
}

func (c *Configuration) save() error {
	serialized, err := json.Marshal(c)
	if err != nil {
		return err
	}
	ioutil.WriteFile(c.configPath, serialized, 0600)
	return nil
}

func (c *Configuration) getRemote(name string) (Remote, error) {
	var r Remote
	var ok bool
	r, ok = c.DMRemotes[name]
	if !ok {
		r, ok = c.S3Remotes[name]
		if !ok {
			return nil, fmt.Errorf("Unable to find remote '%s'", name)
		}
	}
	return r, nil
}

func (c *Configuration) GetRemote(name string) (Remote, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.getRemote(name)
}

// todo this should probably return interfaces and just make a map of all of them
func (c *Configuration) GetRemotes() map[string]*DMRemote {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.DMRemotes
}

func (c *Configuration) GetS3Remotes() map[string]*S3Remote {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.S3Remotes
}

func (c *Configuration) GetCurrentRemote() string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.CurrentRemote
}

func (c *Configuration) SetCurrentRemote(remote string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.DMRemotes[remote]
	if !ok {
		if _, ok = c.S3Remotes[remote]; ok {
			return fmt.Errorf("Cannot switch to remote '%s' - is an S3 remote", remote)
		} else {
			return fmt.Errorf("No such remote '%s'", remote)
		}
	}
	c.CurrentRemote = remote
	return c.save()
}

func (c *Configuration) CurrentVolume() (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.currentVolume()
}

func (c *Configuration) currentVolume() (string, error) {
	r, ok := c.DMRemotes[c.CurrentRemote]
	if !ok {
		return "", fmt.Errorf(
			"Unable to find remote '%s', which was apparently current",
			c.CurrentRemote,
		)
	}
	return r.CurrentVolume, nil
}

func (c *Configuration) SetCurrentVolume(volume string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.DMRemotes[c.CurrentRemote]
	if !ok {
		return fmt.Errorf(
			"Unable to find remote '%s', which was apparently current",
			c.CurrentRemote,
		)
	}

	// Canonicalise
	if strings.HasPrefix(volume, "admin/") {
		volume = strings.TrimPrefix(volume, "admin/")
	}

	(*c.DMRemotes[c.CurrentRemote]).CurrentVolume = volume
	return c.save()
}

func (c *Configuration) DefaultRemoteVolumeFor(peer, namespace, volume string) (string, string, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	remote, err := c.getRemote(peer)
	if err != nil {
		// TODO should we return an error instead of bool? this is getting messy
		return "", "", false
	}
	return remote.DefaultRemoteVolumeFor(namespace, volume)

}

func (c *Configuration) SetPrefixesFor(peer, namespace, volume string, prefixes []string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	remote, err := c.getRemote(peer)
	if err != nil {
		return fmt.Errorf(
			"Unable to find remote '%s'",
			peer,
		)
	}
	s3Remote, ok := remote.(*S3Remote)
	if ok {
		s3Remote.SetPrefixesFor(namespace, volume, prefixes)
	}
	return c.save()
}

func (c *Configuration) SetDefaultRemoteVolumeFor(peer, namespace, volume, remoteNamespace, remoteVolume string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	remote, err := c.getRemote(peer)
	if err != nil {
		return fmt.Errorf(
			"Unable to find remote '%s'",
			peer,
		)
	}
	remote.SetDefaultRemoteVolumeFor(namespace, volume, remoteNamespace, remoteVolume)
	return c.save()
}

func (c *Configuration) CurrentBranchFor(volume string) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	currentBranch, ok := c.DMRemotes[c.CurrentRemote].CurrentBranches[volume]
	if !ok {
		return DEFAULT_BRANCH, nil
	}
	return currentBranch, nil
}

func (c *Configuration) CurrentBranch() (string, error) {
	c.lock.Lock()
	cur, err := c.currentVolume()
	c.lock.Unlock()
	if err != nil {
		return "", err
	}
	return c.CurrentBranchFor(cur)
}

func (c *Configuration) SetCurrentBranch(branch string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	cur, err := c.currentVolume()
	if err != nil {
		return err
	}
	c.DMRemotes[c.CurrentRemote].CurrentBranches[cur] = branch
	return c.save()
}

func (c *Configuration) DeleteStateForVolume(volume string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.DMRemotes[c.CurrentRemote]
	if !ok {
		return fmt.Errorf(
			"Unable to find remote '%s', which was apparently current",
			c.CurrentRemote,
		)
	}
	delete(c.DMRemotes[c.CurrentRemote].CurrentBranches, volume)
	if volume == c.DMRemotes[c.CurrentRemote].CurrentVolume {
		c.DMRemotes[c.CurrentRemote].CurrentVolume = ""
	}
	n, v, err := ParseNamespacedVolume(volume)
	if err == nil {
		delete(c.DMRemotes[c.CurrentRemote].DefaultRemoteVolumes[n], v)
	} else {
		return err
	}
	return c.save()
}

func (c *Configuration) SetCurrentBranchForVolume(volume, branch string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.DMRemotes[c.CurrentRemote]
	if !ok {
		return fmt.Errorf(
			"Unable to find remote '%s', which was apparently current",
			c.CurrentRemote,
		)
	}
	if c.DMRemotes[c.CurrentRemote].CurrentBranches == nil {
		c.DMRemotes[c.CurrentRemote].CurrentBranches = map[string]string{}
	}
	c.DMRemotes[c.CurrentRemote].CurrentBranches[volume] = branch
	return c.save()
}

func (c *Configuration) RemoteExists(remote string) bool {
	_, ok := c.DMRemotes[remote]
	if !ok {
		_, ok = c.S3Remotes[remote]
	}
	return ok
}

func (c *Configuration) AddS3Remote(remote, keyID, secretKey, endpoint string) error {
	ok := c.RemoteExists(remote)
	if ok {
		return fmt.Errorf("Remote exists '%s'", remote)
	}
	c.S3Remotes[remote] = &S3Remote{
		KeyID:     keyID,
		SecretKey: secretKey,
		Endpoint:  endpoint,
	}
	return c.save()
}

func (c *Configuration) AddRemote(remote, user, hostname string, port int, apiKey string) error {
	ok := c.RemoteExists(remote)
	if ok {
		return fmt.Errorf("Remote exists '%s'", remote)
	}
	c.DMRemotes[remote] = &DMRemote{
		User:     user,
		Hostname: hostname,
		Port:     port,
		ApiKey:   apiKey,
	}
	return c.save()
}

func (c *Configuration) RemoveRemote(remote string) error {
	_, ok := c.DMRemotes[remote]
	if !ok {
		_, ok = c.S3Remotes[remote]
		if ok {
			delete(c.S3Remotes, remote)
		} else {
			return fmt.Errorf("No such remote '%s'", remote)
		}
	} else {
		delete(c.DMRemotes, remote)
	}
	if c.CurrentRemote == remote {
		c.CurrentRemote = ""
	}
	return c.save()
}

func (c *Configuration) ClusterFromRemote(remote string, verbose bool) (*JsonRpcClient, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	remoteCreds, ok := c.DMRemotes[remote]
	if !ok {
		return nil, fmt.Errorf("No such remote '%s'", remote)
	}
	return &JsonRpcClient{
		User:     remoteCreds.User,
		Hostname: remoteCreds.Hostname,
		Port:     remoteCreds.Port,
		ApiKey:   remoteCreds.ApiKey,
		Verbose:  verbose,
	}, nil
}

func (c *Configuration) ClusterFromCurrentRemote(verbose bool) (*JsonRpcClient, error) {
	return c.ClusterFromRemote(c.CurrentRemote, verbose)
}
