package hashing

import (
	"strings"
	"testing"
)

// Known-answer values for hashing testing
const (
	helloSHA256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	helloSHA512 = "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
)

func TestHashReader_SHA256(t *testing.T) {
	expectedHash, err := HashReader(strings.NewReader("hello"), "sha256")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expectedHash != helloSHA256 {
		t.Errorf("sha256(hello) = %q, want %q", expectedHash, helloSHA256)
	}
}

func TestHashReader_SHA512(t *testing.T) {
	expectedHash, err := HashReader(strings.NewReader("hello"), "sha512")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expectedHash != helloSHA512 {
		t.Errorf("sha512(hello) = %q, want %q", expectedHash, helloSHA512)
	}
}

// Empty content is valid and must hash without error.
func TestHashReader_Empty(t *testing.T) {
	expectedHash, err := HashReader(strings.NewReader(""), "sha256")
	if err != nil {
		t.Fatalf("unexpected error on empty input: %v", err)
	}
	if expectedHash != emptySHA256 {
		t.Errorf("sha256(empty) = %q, want %q", expectedHash, emptySHA256)
	}
}

// Identical content must produce identical hashes; different content must not.
func TestHashReader_Deterministic(t *testing.T) {
	same1, err := HashReader(strings.NewReader("same content"), "sha256")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	same2, err := HashReader(strings.NewReader("same content"), "sha256")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if same1 != same2 {
		t.Errorf("identical content produced different hashes: %q vs %q", same1, same2)
	}

	other, err := HashReader(strings.NewReader("other content"), "sha256")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if same1 == other {
		t.Errorf("different content produced identical hashes: %q", same1)
	}
}

func TestHashReader_UnsupportedAlgo(t *testing.T) {
	_, err := HashReader(strings.NewReader("hello"), "crc32")
	if err == nil {
		t.Fatal("expected error for unsupported algo, got nil")
	}
}

func TestNewHasher(t *testing.T) {
	for _, algo := range []string{"sha256", "sha512"} {
		if _, err := NewHasher(algo); err != nil {
			t.Errorf("NewHasher(%q) returned error: %v", algo, err)
		}
	}
	if _, err := NewHasher("nope"); err == nil {
		t.Error("expected error for unsupported algo")
	}
}
