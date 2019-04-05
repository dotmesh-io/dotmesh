package test

import (
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/client"
)

func TestUserPriviledged(t *testing.T) {
	dm, err := client.NewDotmeshAPI("./fixtures/admin.config", true)
	if err != nil {
		t.Errorf("Unable to get an instance of API: %s", err)
	}

	priv := dm.IsUserPriveledged()
	if priv != true {
		t.Errorf("Expected privileged mode to be %v instead got %v", true, priv)
	}
}

func TestUserNotPriviledged(t *testing.T) {
	dm, err := client.NewDotmeshAPI("./fixtures/non-admin.config", true)
	if err != nil {
		t.Errorf("Unable to get an instance of API: %s", err)
	}

	priv := dm.IsUserPriveledged()
	if priv != false {
		t.Errorf("Expected privileged mode to be %v instead got %v", false, priv)
	}
}
