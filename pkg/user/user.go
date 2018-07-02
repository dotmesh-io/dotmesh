package user

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/nu7hatch/gouuid"
	"golang.org/x/crypto/scrypt"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/dotmesh-io/dotmesh/pkg/kv"
	"github.com/dotmesh-io/dotmesh/pkg/validator"
)

// special admin user with global privs
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"

// How many bytes of entropy in an API key
const API_KEY_BYTES = 32

// UsersPrefix - KV store prefix for users
const UsersPrefix = "users"

type User struct {
	Id       string
	Name     string
	Email    string
	Salt     []byte
	Password []byte
	ApiKey   string
	Metadata map[string]string
}

func (user User) String() string {
	v := reflect.ValueOf(user)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type Query struct {
	Ref      string // ID, name, email
	Selector string // K8s style selector to filter based on user metadata fields
}

type UserManager interface {
	New(name, email, password string) (*User, error)
	Get(q *Query) (*User, error)
	Update(user *User) error

	UpdatePassword(id string, password string) (*User, error)
	ResetAPIKey(id string) (*User, error)

	Delete(id string) error
	List() ([]*User, error)

	Authenticate(username, password string) (*User, error)
}

type DefaultManager struct {
	kv kv.KV
}

func New(kv kv.KV) *DefaultManager {
	return &DefaultManager{
		kv: kv,
	}
}

func (m *DefaultManager) New(username, email, password string) (*User, error) {
	_, err := m.Get(&Query{Ref: username})
	if err == nil {
		return nil, fmt.Errorf("Username already exists - contact help@dotmesh.io")
	}
	_, err = m.getByEmail(email)
	if err == nil {
		return nil, fmt.Errorf("Email already exists - contact help@dotmesh.io")
	}

	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	salt, hashedPassword, err := hashPassword(password)
	if err != nil {
		return nil, err
	}

	apiKeyBytes := make([]byte, API_KEY_BYTES)
	_, err = rand.Read(apiKeyBytes)
	if err != nil {
		return nil, err
	}

	apiKey := base32.StdEncoding.EncodeToString(apiKeyBytes)

	u := User{
		Id:       id.String(),
		Name:     username,
		Salt:     salt,
		Password: hashedPassword,
		ApiKey:   apiKey,
		Metadata: make(map[string]string),
	}

	bts, err := json.Marshal(&u)
	if err != nil {
		return nil, err
	}

	_, err = m.kv.CreateWithIndex(UsersPrefix, u.Id, u.Name, string(bts))
	if err != nil {
		return nil, err
	}

	return &u, nil
}

func (m *DefaultManager) Update(user *User) (*User, error) {
	bts, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	_, err = m.kv.Set(UsersPrefix, user.Id, string(bts))
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (m *DefaultManager) UpdatePassword(username string, password string) (*User, error) {
	u, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, err
	}
	salt, hashedPassword, err := hashPassword(password)

	if err != nil {
		return nil, err
	}

	u.Salt = salt
	u.Password = hashedPassword

	return m.Update(u)

}

func (m *DefaultManager) ResetAPIKey(username string) (*User, error) {
	u, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, err
	}

	keyBytes := make([]byte, API_KEY_BYTES)
	_, err = rand.Read(keyBytes)
	if err != nil {
		return nil, err
	}
	u.ApiKey = base32.StdEncoding.EncodeToString(keyBytes)

	return m.Update(u)
}

func (m *DefaultManager) Authenticate(username, password string) (*User, error) {
	user, err := m.Get(&Query{Ref: username})
	if err != nil {
		return nil, err
	}

	// See if API key matches
	apiKeyMatch := subtle.ConstantTimeCompare(
		[]byte(user.ApiKey),
		[]byte(user.Password)) == 1

	if apiKeyMatch {
		return user, nil
	}
	// checking password
	hashedPassword, err := scrypt.Key([]byte(password), user.Salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, HASH_BYTES)

	if err != nil {
		return nil, err
	}

	passwordMatch := subtle.ConstantTimeCompare(
		[]byte(user.Password),
		[]byte(hashedPassword)) == 1

	if passwordMatch {
		return user, nil
	}

	return nil, fmt.Errorf("Username or password doesn't match")
}

func (m *DefaultManager) Get(q *Query) (*User, error) {

	if q.Selector != "" {
		return m.getBySelector(q.Selector)
	}

	if validator.IsEmail(q.Ref) {
		return m.getByEmail(q.Ref)
	}

	u, err := m.kv.Get(UsersPrefix, q.Ref)
	if err != nil {
		return nil, err
	}

	var user User
	err = json.Unmarshal([]byte(u.Value), &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (m *DefaultManager) Delete(id string) error {
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

	return m.kv.Delete(UsersPrefix, user.Id, false)
}

func (m *DefaultManager) getByEmail(email string) (*User, error) {
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

func (m *DefaultManager) getBySelector(selector string) (*User, error) {
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

func (m *DefaultManager) List(selector string) ([]*User, error) {
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
