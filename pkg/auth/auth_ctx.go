package auth

import (
	"context"
	"net/http"

	"github.com/dotmesh-io/dotmesh/pkg/user"
)

const authenticationUserIDContextKey = "authenticated-user-id"
const authenticationPasswordAuthContextKey = "authentication-type"

// getAccountID returns current user for this request
func GetUserID(r *http.Request) (id string) {
	if accountID := r.Context().Value(authenticationUserIDContextKey); accountID != nil {
		return accountID.(string)
	}
	return ""
}

func GetAuthenticationType(r *http.Request) user.AuthenticationType {
	if accountID := r.Context().Value(authenticationPasswordAuthContextKey); accountID != nil {
		return accountID.(user.AuthenticationType)
	}
	return user.AuthenticationTypeNone
}

// setUserID sets user for this request
func SetAuthenticationDetails(r *http.Request, id string, authenticationType user.AuthenticationType) *http.Request {
	ctx := context.WithValue(r.Context(), authenticationUserIDContextKey, id)
	ctx = context.WithValue(ctx, authenticationPasswordAuthContextKey, authenticationType)
	return r.WithContext(ctx)
}
