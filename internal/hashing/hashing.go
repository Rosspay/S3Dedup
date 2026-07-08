package hashing

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

func HashReader(r io.Reader, hashAlgo string) (string, error) {
	h, err := NewHasher(hashAlgo)
	if err != nil {
		return "", fmt.Errorf("Hashing algo error: %w", err)
	}
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("hashing content: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func NewHasher(algo string) (hash.Hash, error) {
	switch algo {
	case "sha256":
		return sha256.New(), nil
	case "sha512":
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("Unsupported hashing algo: %s", algo)
	}
}
