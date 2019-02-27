package validator

import (
	"errors"
	"fmt"
	"regexp"
)

// url checking consts
const (
	Email       string = "^(((([a-zA-Z]|\\d|[!#\\$%&'\\*\\+\\-\\/=\\?\\^_`{\\|}~]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])+(\\.([a-zA-Z]|\\d|[!#\\$%&'\\*\\+\\-\\/=\\?\\^_`{\\|}~]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])+)*)|((\\x22)((((\\x20|\\x09)*(\\x0d\\x0a))?(\\x20|\\x09)+)?(([\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x7f]|\\x21|[\\x23-\\x5b]|[\\x5d-\\x7e]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])|(\\([\\x01-\\x09\\x0b\\x0c\\x0d-\\x7f]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}]))))*(((\\x20|\\x09)*(\\x0d\\x0a))?(\\x20|\\x09)+)?(\\x22)))@((([a-zA-Z]|\\d|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])|(([a-zA-Z]|\\d|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])([a-zA-Z]|\\d|-|\\.|_|~|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])*([a-zA-Z]|\\d|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])))\\.)+(([a-zA-Z]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])|(([a-zA-Z]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])([a-zA-Z]|\\d|-|\\.|_|~|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])*([a-zA-Z]|[\\x{00A0}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFEF}])))\\.?$"
	UUID        string = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
	UUIDPattern string = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

	VolumeNamePattern      string = `^[a-zA-Z0-9_\-]{1,64}$`
	VolumeNamespacePattern string = `^[a-zA-Z0-9_\-]{1,64}$`
	BranchPattern          string = `^[a-zA-Z0-9_\-]{1,64}$`
	SubDotPattern          string = `^[a-zA-Z0-9_\-]{1,64}$`
)

var (
	rxUUID      = regexp.MustCompile(UUID)
	rxEmail     = regexp.MustCompile(Email)
	rxNamespace = regexp.MustCompile(VolumeNamespacePattern)
	rxName      = regexp.MustCompile(VolumeNamePattern)
	rxBranch    = regexp.MustCompile(BranchPattern)
	rxSubdot    = regexp.MustCompile(SubDotPattern)
)

// errors
var (
	ErrEmptyName            = errors.New("name cannot be empty")
	ErrEmptyNamespace       = errors.New("namespace cannot be empty")
	ErrEmptySubdot          = errors.New("subdot cannot be empty")
	ErrInvalidVolumeName    = fmt.Errorf("invalid dot name, should match pattern: %s", VolumeNamePattern)
	ErrInvalidNamespaceName = fmt.Errorf("invalid namespace name, should match pattern: %s", VolumeNamespacePattern)
	ErrInvalidBranchName    = fmt.Errorf("invalid branch name, should match pattern: %s", BranchPattern)
	ErrInvalidSubdotName    = fmt.Errorf("invalid subdot name, should match pattern: %s", SubDotPattern)
)

// IsUUID check if the string is a UUID (version 3, 4 or 5).
func IsUUID(str string) bool {
	return rxUUID.MatchString(str)
}

// IsEmail check if the string is an email.
func IsEmail(str string) bool {
	return rxEmail.MatchString(str)
}

// IsValidPassword - checks is user supplied password is valid
func IsValidPassword(str string) (errs []string) {
	if len(str) == 0 {
		errs = append(errs, "password cannot be empty")
	}

	if len(str) < 7 {
		errs = append(errs, "password length should be at least 7 symbols")
	}
	return errs
}

func IsValidVolume(namespace, name string) error {
	err := IsValidVolumeNamespace(namespace)
	if err != nil {
		return err
	}

	return IsValidVolumeName(name)
}

func IsValidVolumeName(str string) error {
	if str == "" {
		return ErrEmptyName
	}

	if !rxName.MatchString(str) {
		// TODO: add human readable error message
		return ErrInvalidVolumeName
	}

	return nil
}

func IsValidVolumeNamespace(str string) error {
	if str == "" {
		return ErrEmptyNamespace
	}

	if !rxNamespace.MatchString(str) {
		return ErrInvalidNamespaceName
	}

	return nil
}

func IsValidBranchName(str string) error {
	if str != "" && !rxBranch.MatchString(str) {
		return ErrInvalidBranchName
	}

	return nil
}

func IsValidSubdotName(str string) error {
	if str == "" {
		return ErrEmptySubdot
	}

	if !rxSubdot.MatchString(str) {
		return ErrInvalidSubdotName
	}

	return nil
}
