package auth

import (
	"net/http"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/user"
)

func TestSetAuthenticationCtxA(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://google.com", nil)

	req = SetAuthenticationDetails(req, "user-x", user.AuthenticationTypePassword)

	if GetUserID(req) != "user-x" {
		t.Errorf("unexpected user ID: %s", GetUserID(req))
	}
}

func TestSetAuthenticationCtxB(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://google.com", nil)

	req = SetAuthenticationDetails(req, "user-y", user.AuthenticationTypeAPIKey)

	if GetUserID(req) != "user-y" {
		t.Errorf("unexpected user ID: %s", GetUserID(req))
	}

	if GetAuthenticationType(req) != user.AuthenticationTypeAPIKey {
		t.Errorf("unexpected authentication type: %s", GetAuthenticationType(req))
	}
}
