package user

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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

	handler := func(rw http.ResponseWriter, req *http.Request) {
		l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
		l.Debug("[ExternalUserManagerServer] request received")
		switch fmt.Sprintf("%s %s", req.Method, req.URL.Path) {
		case "PUT /user/admin":
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
		case "PUT /user":
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
		case "GET /user":
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
		case "POST /user":
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
		case "PUT /user/import":
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
		case "POST /user/password":
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
		case "POST /user/api-key":
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
		case "DELETE /user":
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
		case "GET /user/list":
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
		case "POST /user/authenticate":
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
		case "POST /authorize":
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
		case "POST /authorize-namespace-admin":
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
		default:
			l.Error("Path not found")
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
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
