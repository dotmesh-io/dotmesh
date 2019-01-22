package types

import (
	"crypto/md5"
	"fmt"
	"io"
	"reflect"
)

type User struct {
	Id       string
	Name     string
	Email    string
	Salt     []byte
	Password []byte
	ApiKey   string
	Metadata map[string]string
}

type SafeUser struct {
	Id        string
	Name      string
	Email     string
	EmailHash string
	Metadata  map[string]string
}

// SafeUser - returns safe user by hashing email, removing password and APIKey fields
func (u User) SafeUser() SafeUser {
	h := md5.New()
	io.WriteString(h, u.Email)
	emailHash := fmt.Sprintf("%x", h.Sum(nil))
	return SafeUser{
		Id:        u.Id,
		Name:      u.Name,
		Email:     u.Email,
		EmailHash: emailHash,
		Metadata:  u.Metadata,
	}
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
