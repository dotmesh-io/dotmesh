package main

import (
	"context"
	"log"
	"net/http"

	"github.com/dotmesh-io/dotmesh/pkg/user"
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

	u, err := a.userManager.Authenticate(username, password)
	if err != nil {
		log.Printf(
			"[AuthHandler] Error running check on %s: %s:",
			username, err,
		)
		http.Error(w, "Unauthorized.", http.StatusUnauthorized)
		return
	}

	r = r.WithContext(
		context.WithValue(context.WithValue(r.Context(), "authenticated-user-id", u.Id),
			"password-authenticated", true),
	)

	a.subHandler.ServeHTTP(w, r)
}
