package main

import (
	"net/http"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/user"

	log "github.com/sirupsen/logrus"
)

// NewAuthHandler - create new authentication handler
func NewAuthHandler(handler http.Handler, um user.UserManager) http.Handler {
	return &AuthHandler{
		subHandler:  handler,
		userManager: um,
	}
}

// AuthHandler - acts as a middleware that authenticates any incoming request
// and if it's authenticated, adds additional context
type AuthHandler struct {
	subHandler  http.Handler
	userManager user.UserManager
}

func (a *AuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	username, password, ok := r.BasicAuth()
	if !ok {
		http.Error(w, "Unauthorized.", http.StatusUnauthorized)
		return
	}

	u, authenticationType, err := a.userManager.Authenticate(username, password)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"path":     r.URL.Path,
			"username": username,
		}).Warn("auth handler: authentication failed")

		http.Error(w, "Unauthorized.", http.StatusUnauthorized)
		return
	}

	r = auth.SetAuthenticationDetails(r, u, authenticationType)

	a.subHandler.ServeHTTP(w, r)
}
