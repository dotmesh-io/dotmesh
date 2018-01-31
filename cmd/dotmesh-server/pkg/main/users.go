package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/client"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/crypto/scrypt"
)

// The following consts MUST MATCH those defined in cmd/dm/pkg/commands/cluster.go
//
// FIXME: When we have a shared library betwixt client and server, we can put all this in there.

// special admin user with global privs
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"

// How many bytes of entropy in an API key
const API_KEY_BYTES = 32

// And in a salt
const SALT_BYTES = 32

// And in a password hash
const HASH_BYTES = 32

// Scrypt parameters, these are considered good as of 2017 according to https://godoc.org/golang.org/x/crypto/scrypt
const SCRYPT_N = 32768
const SCRYPT_R = 8
const SCRYPT_P = 1

// Returns salt, password hash, error
func HashPassword(password string) ([]byte, []byte, error) {
	salt := make([]byte, SALT_BYTES)
	_, err := rand.Read(salt)

	if err != nil {
		return []byte{}, []byte{}, err
	}

	hashedPassword, err := scrypt.Key([]byte(password), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, HASH_BYTES)

	if err != nil {
		return []byte{}, []byte{}, err
	}

	return salt, hashedPassword, nil
}

// Create a brand new user, with a new user id
func NewUser(name, email, password string) (User, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return User{}, err
	}
	allUsers, err := AllUsers()
	if err != nil {
		return User{}, err
	}
	// TODO make email in error messages below configurable.
	// XXX this way of enforcing username and email address uniqueness is racy
	// with concurrent creation, need to use something CAS-y in etcd.
	for _, user := range allUsers {
		if user.Name == name {
			return User{}, fmt.Errorf("Username already exists - contact help@dotmesh.io")
		}
		if user.Email == email {
			return User{}, fmt.Errorf("Email already exists - contact help@dotmesh.io")
		}
	}

	salt, hashedPassword, err := HashPassword(password)

	if err != nil {
		return User{}, err
	}

	apiKeyBytes := make([]byte, API_KEY_BYTES)
	_, err = rand.Read(apiKeyBytes)
	if err != nil {
		return User{}, err
	}

	apiKey := base32.StdEncoding.EncodeToString(apiKeyBytes)
	return User{Id: id.String(), Name: name, Email: email, Salt: salt, Password: hashedPassword, ApiKey: apiKey}, nil
}

func (u *User) ResetApiKey() error {
	keyBytes := make([]byte, API_KEY_BYTES)
	_, err := rand.Read(keyBytes)
	if err != nil {
		return err
	}
	u.ApiKey = base32.StdEncoding.EncodeToString(keyBytes)
	return nil
}

func (u *User) UpdatePassword(newPassword string) error {
	salt, hashedPassword, err := HashPassword(newPassword)

	if err != nil {
		return err
	}

	u.Salt = salt
	u.Password = hashedPassword
	return nil
}

// Returns salt, password, apiKey, err
func getPasswords(user string) ([]byte, []byte, string, error) {
	users, err := AllUsers()
	if err != nil {
		return []byte{}, []byte{}, "", err
	}
	for _, u := range users {
		if u.Name == user {
			return u.Salt, u.Password, u.ApiKey, nil
		}
	}
	return []byte{}, []byte{}, "", fmt.Errorf("Unable to find user %v", user)
}

// Returns whether the login was good, whether it was done with the password (as opposed to API key), error
func CheckPassword(username, password string) (bool, bool, error) {
	salt, hash, apiKey, err := getPasswords(username)

	if err != nil {
		return false, false, err
	} else {
		// TODO think more about timing attacks

		// See if API key matches
		apiKeyMatch := subtle.ConstantTimeCompare(
			[]byte(apiKey),
			[]byte(password)) == 1

		// See if password matches hash

		hashedPassword, err := scrypt.Key([]byte(password), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, HASH_BYTES)

		if err != nil {
			return false, false, err
		}

		passwordMatch := subtle.ConstantTimeCompare(
			[]byte(hash),
			[]byte(hashedPassword)) == 1

		return (apiKeyMatch || passwordMatch), passwordMatch, nil
	}
}

// Write the user object to etcd
func (u User) Save() error {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(u)
	if err != nil {
		return err
	}
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/users/%s", ETCD_PREFIX, u.Id),
		string(encoded),
		nil,
	)
	return err
}

func AllUsers() ([]User, error) {
	users := []User{}

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return users, err
	}

	// TODO perhaps it would be better to store users in memory rather than
	// fetching the entire list every time.
	allUsers, err := kapi.Get(
		context.Background(),
		fmt.Sprintf("%s/users", ETCD_PREFIX),
		&client.GetOptions{Recursive: true},
	)

	for _, u := range allUsers.Node.Nodes {
		this := User{}
		err := json.Unmarshal([]byte(u.Value), &this)
		if err != nil {
			return users, err
		}
		users = append(users, this)
	}
	return users, nil
}

func GetUserByName(name string) (User, error) {
	// naive
	us, err := AllUsers()
	if err != nil {
		return User{}, err
	}
	for _, u := range us {
		if u.Name == name {
			return u, nil
		}
	}
	return User{}, fmt.Errorf("User name=%v not found", name)
}

func GetUserByEmail(email string) (User, error) {
	// naive
	us, err := AllUsers()
	if err != nil {
		return User{}, err
	}
	for _, u := range us {
		if u.Email == email {
			return u, nil
		}
	}
	return User{}, fmt.Errorf("User email=%v not found", email)
}

func GetUserById(id string) (User, error) {
	// naive
	us, err := AllUsers()
	if err != nil {
		return User{}, err
	}
	for _, u := range us {
		if u.Id == id {
			return u, nil
		}
	}
	return User{}, fmt.Errorf("User id=%v not found", id)
}

func GetUserByCustomerId(id string) (User, error) {
	// naive
	us, err := AllUsers()
	if err != nil {
		return User{}, err
	}
	for _, u := range us {
		if u.CustomerId == id {
			return u, nil
		}
	}
	return User{}, fmt.Errorf("User customerId=%v not found", id)
}

func (t TopLevelFilesystem) AuthorizeOwner(ctx context.Context) (bool, error) {
	return t.authorize(ctx, false)
}

func (t TopLevelFilesystem) Authorize(ctx context.Context) (bool, error) {
	return t.authorize(ctx, true)
}

func (t TopLevelFilesystem) authorize(ctx context.Context, includeCollab bool) (bool, error) {
	authenticatedUserId := ctx.Value("authenticated-user-id").(string)
	if authenticatedUserId == "" {
		return false, fmt.Errorf("No user found in context.")
	}
	// admin user is always authorized (e.g. docker daemon). users and auth are
	// only really meaningful over the network for data synchronization, when a
	// dotmesh cluster is being used like a hub.
	if authenticatedUserId == ADMIN_USER_UUID {
		return true, nil
	}
	user, err := GetUserById(authenticatedUserId)
	if err != nil {
		return false, err
	}
	if user.Id == t.Owner.Id {
		return true, nil
	}
	if includeCollab {
		for _, other := range t.Collaborators {
			if user.Id == other.Id {
				return true, nil
			}
		}
	}
	return false, nil
}

func UserIsNamespaceAdministrator(userId, namespace string) (bool, error) {
	// Admin gets to administer every namespace
	if userId == ADMIN_USER_UUID {
		return true, nil
	}

	// Otherwise, look up the user...
	user, err := GetUserById(userId)
	if err != nil {
		return false, err
	}

	// ...and see if their name matches the namespace name. In future,
	// this can be extended to cover more configurable rules.
	if user.Name == namespace {
		return true, nil
	} else {
		return false, nil
	}
}

func AuthenticatedUserIsNamespaceAdministrator(ctx context.Context, namespace string) (bool, error) {
	u := ctx.Value("authenticated-user-id").(string)
	if u == "" {
		return false, fmt.Errorf("No user found in context.")
	}

	a, err := UserIsNamespaceAdministrator(u, namespace)
	return a, err
}
