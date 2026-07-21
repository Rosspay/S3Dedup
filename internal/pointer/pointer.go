package pointer

import (
	"encoding/json"
	"fmt"
	"io"
)

const ContentPointerType = ".json+pointer"

type Pointer struct {
	BlobBucket  string `json:"blob_bucket"`
	BlobKey     string `json:"blob_key"`
	Hash        string `json:"hash"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type,omitempty"`
}

func WritePointer(pointer Pointer) ([]byte, error) {
	err := ValidatePointer(pointer)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(pointer)
	if err != nil {
		return nil, fmt.Errorf("WritePointer: %w", err)
	}
	return data, nil
}

func ReadPointer(reader io.Reader) (*Pointer, error) {
	var pointer Pointer
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&pointer)
	if err != nil {
		return nil, fmt.Errorf("ReadPointer error: %w", err)
	}
	err = ValidatePointer(pointer)
	if err != nil {
		return nil, err
	}
	return &pointer, nil
}

func ValidatePointer(pointer Pointer) error {
	switch {
	case pointer.BlobBucket == "":
		return fmt.Errorf("ValidatePointer: BlobBucket can't be empty")
	case pointer.BlobKey == "":
		return fmt.Errorf("ValidatePointer: BlobKey can't be empty")
	case pointer.Hash == "":
		return fmt.Errorf("ValidatePointer: Hash can't be empty")
	case pointer.Size < 0:
		return fmt.Errorf("WritePointer: Size must be non-negative")
	}
	return nil
}
