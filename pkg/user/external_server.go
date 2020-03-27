package user

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func readRequestBody(l log.FieldLogger, rw http.ResponseWriter, req *http.Request, body interface{}) bool {
	if err := json.NewDecoder(req.Body).Decode(body); err != nil {
		l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func sendResponse(rw http.ResponseWriter, status int, body interface{}) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)

	if body != nil {
		// Set up the pipe to write data directly into the Reader.
		pr, pw := io.Pipe()

		// Write JSON-encoded data to the Writer end of the pipe.
		// Write in a separate concurrent goroutine, and remember
		// to Close the PipeWriter, to signal to the paired PipeReader
		// that we’re done writing.
		go func() {
			pw.CloseWithError(json.NewEncoder(pw).Encode(body))
		}()

		_, err := io.Copy(rw, pr)
		if err != nil {
			log.WithError(err).Warn("[ExternalServer] error writing response")
		}
	}
}

func StartExternalServer(listenAddr string, stop <-chan struct{}, um UserManager) error {
	ctx := context.Background()

	r := mux.NewRouter()
	r.HandleFunc("/user/admin", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var u User
		if readRequestBody(l, rw, req, &u) {
			err := um.NewAdmin(&u)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] Admin user creation failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nil)
		}
	}).Methods("PUT")

	r.HandleFunc("/user", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var u NewUserRequest
		if readRequestBody(l, rw, req, &u) {
			nu, err := um.New(u.Name, u.Email, u.Password)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] Admin user creation failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nu)
		}
	}).Methods("PUT")

	r.HandleFunc("/user", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var q Query
		if readRequestBody(l, rw, req, &q) {
			u, err := um.Get(&q)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User query")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, u)
		}
	}).Methods("GET")

	r.HandleFunc("/user", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var u User
		if readRequestBody(l, rw, req, &u) {
			nu, err := um.Update(&u)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User update failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nu)
		}
	}).Methods("POST")

	r.HandleFunc("/user/import", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var u User
		if readRequestBody(l, rw, req, &u) {
			err := um.Import(&u)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User import failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nil)
		}
	}).Methods("PUT")

	r.HandleFunc("/user/password", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var r UpdatePasswordRequest
		if readRequestBody(l, rw, req, &r) {
			nu, err := um.UpdatePassword(r.UserID, r.NewPassword)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User password update failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nu)
		}
	}).Methods("POST")

	r.HandleFunc("/user/api-key", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var r ResetAPIKeyRequest
		if readRequestBody(l, rw, req, &r) {
			nu, err := um.ResetAPIKey(r.UserID)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User API key reset failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nu)
		}
	}).Methods("POST")

	r.HandleFunc("/user", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var r DeleteRequest
		if readRequestBody(l, rw, req, &r) {
			err := um.Delete(r.UserID)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User delete failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, nil)
		}
	}).Methods("DELETE")

	r.HandleFunc("/user/list", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var r ListRequest
		if readRequestBody(l, rw, req, &r) {
			us, err := um.List(r.Selector)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] User list failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, us)
		}
	}).Methods("GET")

	r.HandleFunc("/user/authenticate", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var ar AuthenticateRequest
		if readRequestBody(l, rw, req, &ar) {
			u, at, err := um.Authenticate(ar.Username, ar.Password)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] Authentication failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			if u == nil {
				http.Error(rw, "Authentication failed", http.StatusBadRequest)
				return
			}
			sendResponse(rw, http.StatusOK, &AuthenticateResponse{
				User: *u,
				Type: at.String(),
			})
		}
	}).Methods("POST")

	r.HandleFunc("/authorize", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var ar AuthorizeRequest
		if readRequestBody(l, rw, req, &ar) {
			allowed, err := um.Authorize(&ar.User, ar.OwnerAction, &ar.TopLevelFilesystem)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] Authorize failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, &AuthorizeResponse{
				Allowed: allowed,
			})
		}
	}).Methods("POST")

	r.HandleFunc("/authorize-namespace-admin", func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		var ar AuthorizeNamespaceAdminRequest
		if readRequestBody(l, rw, req, &ar) {
			allowed, err := um.UserIsNamespaceAdministrator(&ar.User, ar.Namespace)
			if err != nil {
				l.WithError(err).Error("[ExternalUserManagerServer] Authorize failure")
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			sendResponse(rw, http.StatusOK, &AuthorizeResponse{
				Allowed: allowed,
			})
		}
	}).Methods("POST")

	handler := func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		l.Debug("[ExternalUserManagerServer] request received")
		defer func() {
			if r := recover(); r != nil {
				l.WithField("panic", r).Error("[ExternalUserManagerServer] panic recovered")
				http.Error(rw, fmt.Sprintf("%v", r), http.StatusInternalServerError)
			}
		}()
		r.ServeHTTP(rw, req)
	}

	s := &http.Server{
		Addr:         listenAddr,
		Handler:      http.HandlerFunc(handler),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		<-stop

		s.Shutdown(ctx)
	}()

	err := s.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	} else {
		return err
	}
}
