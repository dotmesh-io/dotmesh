package user

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// special admin user with global privs
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"

// UsersPrefix - KV store prefix for users
const UsersPrefix = "users"

// Alias
type User = types.User
type SafeUser = types.SafeUser
type Query = types.Query

type AuthenticationType int

// Privileged - authentication type that enables
// certain API action
func (at AuthenticationType) Privileged() bool {
	switch at {
	case AuthenticationTypePassword:
		return true
	}
	return false
}

func (at AuthenticationType) String() string {
	switch at {
	case AuthenticationTypeNone:
		return "none"
	case AuthenticationTypePassword:
		return "password"
	case AuthenticationTypeAPIKey:
		return "apikey"
	}
	return "unknown"
}

func AuthenticationTypeFromString(at string) (AuthenticationType, error) {
	switch at {
	case "none":
		return AuthenticationTypeNone, nil
	case "password":
		return AuthenticationTypePassword, nil
	case "apikey":
		return AuthenticationTypeAPIKey, nil
	default:
		return AuthenticationTypeNone, fmt.Errorf("Unknown authentication type %q", at)
	}
}

const (
	AuthenticationTypeNone AuthenticationType = iota
	AuthenticationTypePassword
	AuthenticationTypeAPIKey
	// AuthenticationTypeOAuth
)

type UserManager interface {
	NewAdmin(user *User) error

	New(name, email, password string) (*User, error)
	Get(q *Query) (*User, error)
	Update(user *User) (*User, error)

	// Import user without hashing password or generating API key
	Import(user *User) error

	UpdatePassword(id string, password string) (*User, error)
	ResetAPIKey(id string) (*User, error)

	Delete(id string) error
	List(selector string) ([]*User, error)

	// Authenticate user, if successful returns User struct and
	// authentication type or error if unsuccessful
	Authenticate(username, password string) (*User, AuthenticationType, error)

	// Authorize user action on a tlf, returns (true, nil) for OK, (false, nil) for not OK,
	// and (false, error) for an error happened (so not OK).
	Authorize(user *User, ownerAction bool, tlf *types.TopLevelFilesystem) (bool, error)
}
