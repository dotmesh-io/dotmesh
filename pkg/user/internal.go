package user

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/dotmesh-io/dotmesh/pkg/crypto"
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"
	"github.com/dotmesh-io/dotmesh/pkg/validator"

	log "github.com/sirupsen/logrus"
)

type InternalManager struct {
	kv store.KVStoreWithIndex
}

func NewInternal(kv store.KVStoreWithIndex) *InternalManager {
	return &InternalManager{
		kv: kv,
	}
}

func (m *InternalManager) NewAdmin(user *User) error {

	log.WithFields(log.Fields{
		"id":   user.Id,
		"name": user.Name,
	}).Info("user manager: creating admin account")

	salt, hashedPassword, err := crypto.HashPassword(string(user.Password))
	if err != nil {
		return err
	}
	user.Salt = salt
	user.Password = hashedPassword

	if user.ApiKey == "" {
		apiKey, err := crypto.GenerateAPIKey()
		if err != nil {
			return err
		}
		user.ApiKey = apiKey
	}

	bts, err := json.Marshal(&user)
	if err != nil {
		return err
	}

	_, err = m.kv.CreateWithIndex(UsersPrefix, user.Id, user.Name, bts)
	return err
}

func (m *InternalManager) New(username, email, password string) (*User, error) {
	_, err := m.Get(&Query{Ref: username})
	if err == nil {
		return nil, fmt.Errorf("Username already exists - contact help@dotmesh.io")
	}
	_, err = m.getByEmail(email)
	if err == nil {
		return nil, fmt.Errorf("Email already exists - contact help@dotmesh.io")
	}

	salt, hashedPassword, err := crypto.HashPassword(password)
	if err != nil {
		return nil, err
	}

	apiKey, err := crypto.GenerateAPIKey()
	if err != nil {
		return nil, err
	}

	u := User{
		Id:       uuid.New().String(),
		Name:     username,
		Email:    email,
		Salt:     salt,
		Password: hashedPassword,
		ApiKey:   apiKey,
		Metadata: make(map[string]string),
	}

	bts, err := json.Marshal(&u)
	if err != nil {
		return nil, err
	}

	_, err = m.kv.CreateWithIndex(UsersPrefix, u.Id, u.Name, bts)
	if err != nil {
		return nil, err
	}

	return &u, nil
}

func (m *InternalManager) Update(user *User) (*User, error) {
	bts, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	_, err = m.kv.Set(UsersPrefix, user.Id, bts)
	if err != nil {
		return nil, err
	}
	return user, nil
}

// Import - imports user without generating password, API key
func (m *InternalManager) Import(user *User) error {

	if user.Id == "" {
		return fmt.Errorf("user ID not set")
	}

	if user.Name == "" {
		return fmt.Errorf("user name not set")
	}

	if len(user.Password) == 0 {
		return fmt.Errorf("user password not set")
	}

	if len(user.Salt) == 0 {
		return fmt.Errorf("user password salt not set")
	}

	bts, err := json.Marshal(user)
	if err != nil {
		return err
	}
	_, err = m.kv.Set(UsersPrefix, user.Id, bts)
	if err != nil {
		return err
	}

	m.kv.AddToIndex(UsersPrefix, user.Name, user.Id)

	return nil
}

func (m *InternalManager) UpdatePassword(username string, password string) (*User, error) {
	u, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, err
	}
	salt, hashedPassword, err := crypto.HashPassword(password)

	if err != nil {
		return nil, err
	}

	u.Salt = salt
	u.Password = hashedPassword

	return m.Update(u)

}

func (m *InternalManager) ResetAPIKey(username string) (*User, error) {
	u, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, err
	}

	apiKey, err := crypto.GenerateAPIKey()
	if err != nil {
		return nil, err
	}

	u.ApiKey = apiKey

	return m.Update(u)
}

func (m *InternalManager) Authenticate(username, password string) (*User, AuthenticationType, error) {
	user, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, AuthenticationTypeNone, err
	}

	if user.ApiKey == password {
		return user, AuthenticationTypeAPIKey, nil
	}

	passwordMatch, err := crypto.PasswordMatches(user.Salt, password, string(user.Password))
	if err != nil {
		return nil, AuthenticationTypeNone, err
	}

	if passwordMatch {
		return user, AuthenticationTypePassword, nil
	}

	return nil, AuthenticationTypeNone, fmt.Errorf("Username or password doesn't match")
}

func (m *InternalManager) Get(q *Query) (*User, error) {

	if q.Selector != "" {
		return m.getBySelector(q.Selector)
	}

	if validator.IsEmail(q.Ref) {
		return m.getByEmail(q.Ref)
	}

	u, err := m.kv.Get(UsersPrefix, q.Ref)
	if err != nil {

		return m.fullSearch(q.Ref)
	}

	var user User
	err = json.Unmarshal([]byte(u.Value), &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (m *InternalManager) fullSearch(ref string) (*User, error) {
	users, err := m.List("")
	if err != nil {
		return nil, err
	}

	for _, u := range users {
		if u.Name == ref {
			// adding user to the index for later faster lookup
			err = m.kv.AddToIndex(UsersPrefix, u.Name, u.Id)
			if err != nil {
				log.WithFields(log.Fields{
					"name":  u.Name,
					"id":    u.Id,
					"error": err,
				}).Error("users manager: error while adding user to the index")
			}

			return u, nil
		}
	}
	return nil, fmt.Errorf("User name=%s not found", ref)
}

func (m *InternalManager) Delete(id string) error {
	if !validator.IsUUID(id) {
		return fmt.Errorf("'%s' is not a valid ID", id)
	}

	user, err := m.Get(&Query{Ref: id})
	if err != nil {
		return err
	}

	err = m.kv.DeleteFromIndex(UsersPrefix, user.Name)
	if err != nil {
		// TODO: maybe at least log it
	}

	return m.kv.Delete(UsersPrefix, user.Id)
}

func (m *InternalManager) getByEmail(email string) (*User, error) {
	users, err := m.List("")
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		if u.Email == email {
			return u, nil
		}
	}

	return nil, fmt.Errorf("User email=%s not found", email)
}

func (m *InternalManager) getBySelector(selector string) (*User, error) {
	users, err := m.List(selector)
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("User not found")
	}

	if len(users) > 1 {
		return nil, fmt.Errorf("more than one matches for selector=%s, use List API", selector)
	}

	return users[0], nil
}

func (m *InternalManager) List(selector string) ([]*User, error) {
	sel, err := labels.Parse(selector)
	if err != nil {
		return nil, err
	}
	users := []*User{}
	ns, err := m.kv.List(UsersPrefix)
	if err != nil {
		return users, nil
	}
	for _, n := range ns {
		var user User
		err := json.Unmarshal([]byte(n.Value), &user)
		if err != nil {
			// log

			continue
		}

		if sel.Matches(labels.Set(user.Metadata)) {
			users = append(users, &user)
		}
	}

	return users, nil
}

func (m *InternalManager) Authorize(user *User, collabsAllowed bool, tlf *types.TopLevelFilesystem) (bool, error) {
	// admin user is always authorized (e.g. docker daemon). users and auth are
	// only really meaningful over the network for data synchronization, when a
	// dotmesh cluster is being used like a hub.
	if user.Id == ADMIN_USER_UUID {
		return true, nil
	}
	if user.Id == tlf.Owner.Id {
		return true, nil
	}
	if collabsAllowed {
		for _, other := range tlf.Collaborators {
			if user.Id == other.Id {
				return true, nil
			}
		}
	}
	return false, nil
}

func (m *InternalManager) UserIsNamespaceAdministrator(user *User, namespace string) (bool, error) {
	// Admin gets to administer every namespace
	if user.Id == ADMIN_USER_UUID {
		return true, nil
	}

	// ...and see if their name matches the namespace name.
	if user.Name == namespace {
		return true, nil
	} else {
		return false, nil
	}
}
