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

func readRequestBody(req *http.Request, body interface{}) error {
	if err := json.NewDecoder(req.Body).Decode(body); err != nil {
		return err
	}
	return nil
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
	s := &http.Server{
		Addr: listenAddr,
		Handler: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			l := log.WithField("path", req.URL.Path).WithField("method", req.Method)
			l.Debug("[ExternalUserManagerServer] request received")
			switch fmt.Sprintf("%s %s", req.Method, req.URL.Path) {
			case "PUT /user/admin":
				var u User
				err := readRequestBody(req, &u)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					err := um.NewAdmin(&u)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] Admin user creation failure")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						sendResponse(rw, http.StatusOK, nil)
					}
				}
			case "PUT /user":
				var u NewUserRequest
				err := readRequestBody(req, &u)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					nu, err := um.New(u.Name, u.Email, u.Password)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] Admin user creation failure")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						sendResponse(rw, http.StatusOK, nu)
					}
				}
			case "GET /user":
				var q Query
				err := readRequestBody(req, &q)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					u, err := um.Get(&q)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] User query")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						sendResponse(rw, http.StatusOK, u)
					}
				}
			case "POST /user/authenticate":
				var ar AuthenticateRequest
				err := readRequestBody(req, &ar)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					u, at, err := um.Authenticate(ar.Username, ar.Password)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] Authentication failure")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						if u == nil {
							http.Error(rw, "Authentication failed", http.StatusBadRequest)
						} else {
							sendResponse(rw, http.StatusOK, &AuthenticateResponse{
								User: *u,
								Type: at.String(),
							})
						}
					}
				}
			case "POST /authorize":
				var ar AuthorizeRequest
				err := readRequestBody(req, &ar)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					allowed, err := um.Authorize(&ar.User, ar.OwnerAction, &ar.TopLevelFilesystem)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] Authorize failure")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						sendResponse(rw, http.StatusOK, &AuthorizeResponse{
							Allowed: allowed,
						})
					}
				}
			case "POST /authorize-namespace-admin":
				var ar AuthorizeNamespaceAdminRequest
				err := readRequestBody(req, &ar)
				if err != nil {
					l.WithError(err).Error("[ExternalUserManagerServer] Bad request")
					http.Error(rw, err.Error(), http.StatusBadRequest)
				} else {
					allowed, err := um.UserIsNamespaceAdministrator(&ar.User, ar.Namespace)
					if err != nil {
						l.WithError(err).Error("[ExternalUserManagerServer] Authorize failure")
						http.Error(rw, err.Error(), http.StatusInternalServerError)
					} else {
						sendResponse(rw, http.StatusOK, &AuthorizeResponse{
							Allowed: allowed,
						})
					}
				}
			default:
				l.Error("Path not found")
				http.Error(rw, "Not Found", http.StatusNotFound)
			}
		}),
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
