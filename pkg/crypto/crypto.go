package crypto

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"

	"golang.org/x/crypto/scrypt"
)

// Keygen parameters
const (
	saltBytes = 32
	hashBytes = 32
	// How many bytes of entropy in an API key
	apiKeyBytes = 32
	// Scrypt parameters, these are considered good as of 2017 according to https://godoc.org/golang.org/x/crypto/scrypt
	scryptN = 32768
	scryptR = 8
	scryptP = 1
)

// HashPassword - hashes password
func HashPassword(password string) ([]byte, []byte, error) {
	salt := make([]byte, saltBytes)
	_, err := rand.Read(salt)
	if err != nil {
		return []byte{}, []byte{}, err
	}

	hashedPassword, err := hash(salt, password)
	if err != nil {
		return []byte{}, []byte{}, err
	}

	return salt, hashedPassword, nil
}

func hash(salt []byte, password string) ([]byte, error) {
	return scrypt.Key([]byte(password), salt, scryptN, scryptR, scryptP, hashBytes)
}

// PasswordMatches - checks whether user supplied password(plain string) matches hash password.
// Requires original hashed password and salt
func PasswordMatches(salt []byte, suppliedPassword, hashedPassword string) (bool, error) {
	hashedSuppliedPassword, err := hash(salt, suppliedPassword)
	if err != nil {
		return false, err
	}

	return subtle.ConstantTimeCompare(
		[]byte(hashedSuppliedPassword),
		[]byte(hashedPassword)) == 1, nil
}

// GenerateAPIKey - generates random API key for later authentication
func GenerateAPIKey() (string, error) {

	apiKeyBytes := make([]byte, apiKeyBytes)
	_, err := rand.Read(apiKeyBytes)
	if err != nil {
		return "", err
	}

	return base32.StdEncoding.EncodeToString(apiKeyBytes), nil
}
