package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"

	"github.com/dotmesh-io/citools"

	log "github.com/sirupsen/logrus"
)

type DummyUserManager struct {
	allowStuff  bool
	theValidKey string
	log         log.FieldLogger
}

func (m *DummyUserManager) NewAdmin(user *user.User) error {
	m.log.Infof("NewAdmin: %#v", *user)
	return nil
}

func (m *DummyUserManager) New(name, email, password string) (*user.User, error) {
	m.log.Infof("New: %q %q %q", name, email, password)
	return &user.User{
		Id:       email,
		Name:     name,
		Email:    email,
		Salt:     []byte{},
		Password: []byte(password),
		ApiKey:   "123",
	}, nil
}
func (m *DummyUserManager) Get(q *user.Query) (*user.User, error) {
	m.log.Infof("Get: %q %q", q.Ref, q.Selector)
	return &user.User{
		Id:   q.Ref,
		Name: q.Ref,
	}, nil
}

func (m *DummyUserManager) Update(user *user.User) (*user.User, error) {
	m.log.Infof("Update: %#v", *user)
	return nil, nil
}

func (m *DummyUserManager) Import(user *user.User) error {
	m.log.Infof("Import: %#v", *user)
	return nil
}

func (m *DummyUserManager) UpdatePassword(id string, password string) (*user.User, error) {
	m.log.Infof("UpdatePassword: %s <- %q", id, password)
	return &user.User{
		Id:       id,
		Password: []byte(password),
	}, nil
}

func (m *DummyUserManager) ResetAPIKey(id string) (*user.User, error) {
	m.log.Infof("ResetAPIKey: %s", id)
	return &user.User{
		Id:     id,
		ApiKey: "456",
	}, nil
}

func (m *DummyUserManager) Delete(id string) error {
	m.log.Infof("Delete: %s", id)
	return nil
}

func (m *DummyUserManager) List(selector string) ([]*user.User, error) {
	m.log.Infof("List: %s", selector)
	return []*user.User{
		&user.User{
			Id:   "00000000-0000-0000-0000-000000000000",
			Name: "admin",
		},
		&user.User{
			Id:   "id-of-user-bob",
			Name: "bob",
		},
	}, nil
}

func (m *DummyUserManager) Authenticate(username, password string) (*user.User, user.AuthenticationType, error) {
	m.log.Infof("Authenticate: %s / %q", username, password)

	if username == "admin" {
		return &user.User{
			Id:   "00000000-0000-0000-0000-000000000000",
			Name: username,
		}, user.AuthenticationTypePassword, nil
	} else {
		if password == m.theValidKey {
			return &user.User{
				Id:   "id-of-user-" + username,
				Name: username,
			}, user.AuthenticationTypePassword, nil
		} else {
			return nil, user.AuthenticationTypeNone, nil
		}
	}
}

func (m *DummyUserManager) Authorize(user *user.User, ownerAction bool, tlf *types.TopLevelFilesystem) (bool, error) {
	m.log.Infof("Authorize: %#v / %t / %#v", *user, ownerAction, *tlf)
	return m.allowStuff, nil
}

func (m *DummyUserManager) UserIsNamespaceAdministrator(user *user.User, namespace string) (bool, error) {
	m.log.Infof("UserIsNamespaceAdministrator: %#v / %s", *user, namespace)
	return m.allowStuff, nil
}

func TestExternalUserManager(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	serverPort := 12345
	// FIXME: find our actual IP
	serverUrl := fmt.Sprintf("http://%s:%d", "192.168.1.33", serverPort)

	stop := make(chan struct{})
	defer func() {
		stop <- struct{}{}
	}()

	um := DummyUserManager{
		log: log.StandardLogger(),
	}

	go func() {
		err := user.StartExternalServer(fmt.Sprintf(":%d", serverPort), stop, &um)
		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(4 * time.Second)

	f := citools.Federation{
		citools.NewClusterWithEnv(1, map[string]string{"EXTERNAL_USER_MANAGER_URL": serverUrl}), // cluster_0_node_0
		citools.NewCluster(1), // cluster_1_node_0
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatalf("failed to start cluster, error: %s", err)
	}

	// node1 has the external user manager

	// node1 := f[0].GetNode(0).Container
	cluster1Node := f[0].GetNode(0)

	// node2 is normal

	node2 := f[1].GetNode(0).Container
	// cluster2Node := f[1].GetNode(0)

	bobKey := "bob is great"

	// Create user bob on node 1
	err = citools.RegisterUser(cluster1Node, "bob", "bob@bob.com", bobKey)
	if err != nil {
		t.Error(err)
	}

	// tell second node about bob on the first node... our user manager accepts bobKey
	um.theValidKey = bobKey
	citools.RunOnNode(t, node2, "echo '"+bobKey+"' | dm remote add bob bob@"+cluster1Node.IP)

	t.Run("WrongAPIKey", func(t *testing.T) {
		// use wrong apikey
		citools.RunOnNode(t, node2, "if echo 'bob is a bit weird' | dm remote add bob bob@"+cluster1Node.IP+"; then false; else true; fi")
	})

	t.Run("ComputerSaysNo", func(t *testing.T) {
		fsName := citools.UniqName()
		um.allowStuff = false
		citools.RunOnNode(t, node2, "dm remote switch local")

		// push to bob on node 1
		citools.RunOnNode(t, node2, citools.DockerRun(fsName)+" touch /foo/bananas")
		citools.RunOnNode(t, node2, "dm switch "+fsName)
		citools.RunOnNode(t, node2, "dm commit -m'This is bananas'")

		// Can't push as dummy user manager always says no
		citools.RunOnNode(t, node2, "if dm push bob "+fsName+"; then false; else true; fi")

		// Can't delete it even though you own it, because dummy user manager always says no
		citools.RunOnNode(t, node2, "dm remote switch bob")
		citools.RunOnNode(t, node2, "if dm dot delete -f bob/"+fsName+"; then false; else true; fi")
	})

	t.Run("ComputerSaysYes", func(t *testing.T) {
		fsName := citools.UniqName()
		um.allowStuff = true
		citools.RunOnNode(t, node2, "dm remote switch local")

		// push to bob on node 1
		citools.RunOnNode(t, node2, citools.DockerRun(fsName)+" touch /foo/bananas")
		citools.RunOnNode(t, node2, "dm switch "+fsName)
		citools.RunOnNode(t, node2, "dm commit -m'This is bananas'")

		citools.RunOnNode(t, node2, "dm push bob "+fsName)
		citools.RunOnNode(t, node2, "dm pull bob")

		citools.RunOnNode(t, node2, "dm remote switch bob")
		citools.RunOnNode(t, node2, "dm dot delete -f bob/"+fsName)
	})

	t.Run("BackupAndRestore", func(t *testing.T) {
		// We're not so interested in the backup/restore correctly restoring users,
		// as most external user managers will be interfaces to things that are
		// backed up elsewhere - just want to make sure we don't break
		// backup/restore of everything else
		fsName := citools.UniqName()
		um.allowStuff = true
		citools.RunOnNode(t, node2, "dm remote switch local")

		// Push something to back up
		citools.RunOnNode(t, node2, citools.DockerRun(fsName)+" touch /foo/bananas")
		citools.RunOnNode(t, node2, "dm switch "+fsName)
		citools.RunOnNode(t, node2, "dm commit -m'This is bananas'")

		citools.RunOnNode(t, node2, "dm push bob "+fsName)

		// Pull a backup
		citools.RunOnNode(t, node2, "dm remote switch cluster_0_node_0")
		citools.RunOnNode(t, node2, "dm cluster backup-etcd > backup.json")

		// Restore it
		citools.RunOnNode(t, node2, "dm cluster restore-etcd < backup.json")

		// Pull back
		citools.RunOnNode(t, node2, "dm pull bob")
	})
}
