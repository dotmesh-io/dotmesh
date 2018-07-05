package main

import (
	"context"
	"net/http"

	"github.com/dotmesh-io/dotmesh/pkg/user"

	log "github.com/sirupsen/logrus"
)

func NewAuthHandler(handler http.Handler, um user.UserManager) http.Handler {
	return &AuthHandler{
		subHandler:  handler,
		userManager: um,
	}
}

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

	r = r.WithContext(
		context.WithValue(context.WithValue(r.Context(), "authenticated-user-id", u.Id),
			"password-authenticated", authenticationType.Privileged()),
	)

	a.subHandler.ServeHTTP(w, r)
}
