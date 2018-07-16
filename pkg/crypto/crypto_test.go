package crypto

import (
	"testing"
)

func TestHashPassword(t *testing.T) {
	s, h, err := HashPassword("password")
	if err != nil {
		t.Fatalf("failed to gen password: %s", err)
	}

	if len(s) == 0 {
		t.Errorf("missing salt")
	}

	if string(h) == "password" {
		t.Errorf("password wasn't hashed: %s", string(h))
	}
}

func TestHashPasswordEmpty(t *testing.T) {
	_, _, err := HashPassword("")
	if err == nil {
		t.Errorf("expected to fail hash empty password")
	}
}

func TestPasswordMatches(t *testing.T) {
	s, h, err := HashPassword("passwordverysecret")
	if err != nil {
		t.Fatalf("failed to gen password: %s", err)
	}

	matches, err := PasswordMatches(s, "passwordverysecret", string(h))
	if err != nil {
		t.Errorf("match function failed: %s", err)
	}

	if !matches {
		t.Errorf("password should have matched")
	}
}

func TestPasswordMatchesB(t *testing.T) {
	s, h, err := HashPassword("passwordverysecret")
	if err != nil {
		t.Fatalf("failed to gen password: %s", err)
	}

	matches, err := PasswordMatches(s, "another", string(h))
	if err != nil {
		t.Errorf("match function failed: %s", err)
	}

	if matches {
		t.Errorf("password should not match!")
	}
}

func TestPasswordMatchesCEmptySalt(t *testing.T) {
	_, h, err := HashPassword("passwordverysecret")
	if err != nil {
		t.Fatalf("failed to gen password: %s", err)
	}

	matches, err := PasswordMatches([]byte(""), "another", string(h))
	if err == nil {
		t.Errorf("expected error regarding empty salt")
	}

	if matches {
		t.Errorf("password should not match!")
	}
}

func TestGenerateAPIKey(t *testing.T) {
	k, err := GenerateAPIKey()
	if err != nil {
		t.Errorf("failed to gen key: %s", err)
	}

	if len(k) == 0 {
		t.Errorf("key is empty")
	}
}
