package remotes

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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sync"

	"golang.org/x/net/context"

	"github.com/dotmesh-io/rpc/v2/json2"
	opentracing "github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/openzipkin/zipkin-go-opentracing/examples/middleware"
)

type Remote interface {
	DefaultNamespace() string
	DefaultRemoteVolumeFor(string, string) (VolumeName, bool)
	SetDefaultRemoteVolumeFor(string, string, string, string)
}

type S3Remote struct {
	KeyID                string
	SecretKey            string
	DefaultRemoteVolumes map[string]map[string]VolumeName
}

type DMRemote struct {
	User                 string
	Hostname             string
	Port                 int `json:",omitempty"`
	ApiKey               string
	CurrentVolume        string
	CurrentBranches      map[string]string
	DefaultRemoteVolumes map[string]map[string]VolumeName
}

func (remote DMRemote) DefaultNamespace() string {
	return remote.User
}

func (remote S3Remote) DefaultNamespace() string {
	return ""
}

// TODO is there a less hacky way of doing this? hate the duplication, but otherwise you need to cast all over the place
func (remote DMRemote) SetDefaultRemoteVolumeFor(localNamespace, localVolume, remoteNamespace, remoteVolume string) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]VolumeName{}
	}
	remote.DefaultRemoteVolumes[localNamespace][localVolume] = VolumeName{remoteNamespace, remoteVolume}
}

func (remote DMRemote) DefaultRemoteVolumeFor(localNamespace, localVolume string) (VolumeName, bool) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	return volName, ok
}

func (remote S3Remote) SetDefaultRemoteVolumeFor(localNamespace, localVolume, remoteNamespace, remoteVolume string) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]VolumeName{}
	}
	remote.DefaultRemoteVolumes[localNamespace][localVolume] = VolumeName{remoteNamespace, remoteVolume}
}

func (remote S3Remote) DefaultRemoteVolumeFor(localNamespace, localVolume string) (VolumeName, bool) {
	if remote.DefaultRemoteVolumes == nil {
		remote.DefaultRemoteVolumes = map[string]map[string]VolumeName{}
	}
	if remote.DefaultRemoteVolumes[localNamespace] == nil {
		remote.DefaultRemoteVolumes[localNamespace] = map[string]VolumeName{}
	}
	volName, ok := remote.DefaultRemoteVolumes[localNamespace][localVolume]
	return volName, ok
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
	fmt.Printf("Inside get remote")
	r, ok = c.DMRemotes[name]
	if !ok {
		r, ok = c.S3Remotes[name]
		fmt.Printf("%#v", r)
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
	defaultRemoteVolume, ok := remote.DefaultRemoteVolumeFor(namespace, volume)
	if ok {
		return defaultRemoteVolume.Namespace, defaultRemoteVolume.Name, true
	}
	return "", "", false

}

func (c *Configuration) SetDefaultRemoteVolumeFor(peer, namespace, volume, remoteNamespace, remoteVolume string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	remote, err := c.GetRemote(peer)
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

func (c *Configuration) AddS3Remote(remote, keyID, secretKey string) error {
	ok := c.RemoteExists(remote)
	if ok {
		return fmt.Errorf("Remote exists '%s'", remote)
	}
	c.S3Remotes[remote] = &S3Remote{
		KeyID:     keyID,
		SecretKey: secretKey,
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

func (c *Configuration) ClusterFromRemote(remote string) (*JsonRpcClient, error) {
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
	}, nil
}

func (c *Configuration) ClusterFromCurrentRemote() (*JsonRpcClient, error) {
	return c.ClusterFromRemote(c.CurrentRemote)
}

type JsonRpcClient struct {
	User     string
	Hostname string
	Port     int
	ApiKey   string
}

func (jsonRpcClient JsonRpcClient) String() string {
	v := reflect.ValueOf(jsonRpcClient)
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

type Address struct {
	Scheme   string
	Hostname string
	Port     int
}

func (j *JsonRpcClient) tryAddresses(ctx context.Context, as []Address) (Address, error) {
	var errs []error
	for _, a := range as {
		var result bool
		err := j.reallyCallRemote(ctx, "DotmeshRPC.Ping", nil, &result, a)
		if err == nil {
			return a, nil
		} else {
			errs = append(errs, err)
		}
	}
	// TODO distinguish between network errors and API errors here: #356
	return Address{}, fmt.Errorf("Unable to connect to any of the addresses attempted: %+v, errs: %s", as, errs)
}

// call a method with string args, and attempt to decode it into result
func (j *JsonRpcClient) CallRemote(
	ctx context.Context, method string, args interface{}, result interface{},
) error {
	if j == nil {
		return fmt.Errorf(
			"No remote cluster specified. List remotes with 'dm remote -v'. " +
				"Choose one with 'dm remote switch' or create one with 'dm remote " +
				"add'. Try 'dm cluster init' if you don't have a cluster yet.",
		)
	}
	addressToUse := Address{}
	addressesToTry := []Address{}
	var err error
	if j.Port == 0 {
		if j.Hostname == "saas.dotmesh.io" || j.Hostname == "dothub.com" {
			scheme := "https"
			port := 443
			addressesToTry = append(addressesToTry, Address{scheme, j.Hostname, port})
		} else {
			scheme := "http"
			addressesToTry = append(addressesToTry, Address{scheme, j.Hostname, 32607})
			addressesToTry = append(addressesToTry, Address{scheme, j.Hostname, 6969})
		}
		addressToUse, err = j.tryAddresses(ctx, addressesToTry)
		if err != nil {
			return err
		}
	} else {
		addressToUse = Address{"http", j.Hostname, j.Port}
	}

	return j.reallyCallRemote(ctx, method, args, result, addressToUse)
}

func (j *JsonRpcClient) reallyCallRemote(
	ctx context.Context, method string, args interface{}, result interface{},
	addressToUse Address,
) error {
	// create new span using span found in context as parent (if none is found,
	// our span becomes the trace root).
	span, ctx := opentracing.StartSpanFromContext(ctx, method)
	span.LogFields(
		opentracinglog.String("type", "cli-rpc"),
		opentracinglog.String("method", method),
		opentracinglog.String("args", fmt.Sprintf("%v", args)),
	)
	defer span.Finish()

	scheme := addressToUse.Scheme
	hostname := addressToUse.Hostname
	port := addressToUse.Port

	url := fmt.Sprintf("%s://%s:%d/rpc", scheme, hostname, port)
	message, err := json2.EncodeClientRequest(method, args)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(message))
	if err != nil {
		return err
	}

	tracer := opentracing.GlobalTracer()
	// use our middleware to propagate our trace
	req = middleware.ToHTTPRequest(tracer)(req.WithContext(ctx))

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(j.User, j.ApiKey)
	client := new(http.Client)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		// TODO add user mgmt subcommands, then reference them in this error message
		// annotate our span with the error condition
		span.SetTag("error", "Permission denied")
		return fmt.Errorf("Permission denied. Please check that your API key is still valid.")
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.SetTag("error", err.Error())
		return fmt.Errorf("Error reading body: %s", err)
	}
	err = json2.DecodeClientResponse(bytes.NewBuffer(b), &result)
	if err != nil {
		span.SetTag("error", fmt.Sprintf("Response '%s' yields error %s", string(b), err))
		return fmt.Errorf("Response '%s' yields error %s", string(b), err)
	}
	return nil
}
