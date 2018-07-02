package user

import (
	"crypto/rand"

	"golang.org/x/crypto/scrypt"
)

// And in a salt
const SALT_BYTES = 32

// And in a password hash
const HASH_BYTES = 32

// Scrypt parameters, these are considered good as of 2017 according to https://godoc.org/golang.org/x/crypto/scrypt
const SCRYPT_N = 32768
const SCRYPT_R = 8
const SCRYPT_P = 1

func hashPassword(password string) ([]byte, []byte, error) {
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
