package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/client"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/user"
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

	// If there's no record of any users, there are no users.
	if allUsers == nil {
		return users, nil
	}

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

func UserIsNamespaceAdministrator(user *user.User, namespace string) (bool, error) {
	// Admin gets to administer every namespace
	if user.Id == ADMIN_USER_UUID {
		return true, nil
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
	u := auth.GetUserFromCtx(ctx)
	if u == nil {
		return false, fmt.Errorf("No user found in context.")
	}

	a, err := UserIsNamespaceAdministrator(u, namespace)
	return a, err
}
