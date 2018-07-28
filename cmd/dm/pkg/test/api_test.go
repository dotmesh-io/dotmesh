package test

import (
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"testing"
)

func TestUserPriviledged(t *testing.T) {
	dm, err := remotes.NewDotmeshAPI("./fixtures/admin.config", true)
	if err != nil {
		t.Errorf("Unable to get an instance of API: %s", err)
	}

	priv := dm.IsUserPriveledged()
	if priv != true {
		t.Errorf("Expected privileged mode to be %v instead got %v", true, priv)
	}
}

func TestUserNotPriviledged(t *testing.T) {
	dm, err := remotes.NewDotmeshAPI("./fixtures/non-admin.config", true)
	if err != nil {
		t.Errorf("Unable to get an instance of API: %s", err)
	}

	priv := dm.IsUserPriveledged()
	if priv != false {
		t.Errorf("Expected privileged mode to be %v instead got %v", false, priv)
	}
}
