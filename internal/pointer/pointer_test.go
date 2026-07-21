package pointer

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestWritePointerBasic(t *testing.T) {
	pointer := Pointer{
		BlobBucket:  "bucket",
		BlobKey:     "blob/hash",
		Hash:        "hash",
		Size:        100000,
		ContentType: ".png",
	}
	data, err := WritePointer(pointer)
	if err != nil {
		t.Fatalf("WriteJSON fatal error: %v", err)
	}

	var gotPointer Pointer

	decoder := json.NewDecoder(bytes.NewReader(data))
	err = decoder.Decode(&gotPointer)
	if err != nil {
		t.Fatalf("Decoding file error: %v", err)
	}

	isEqual := cmp.Equal(&pointer, &gotPointer)
	if !isEqual {
		t.Error("WritePointer basic test failed")
	}
}

func TestWritePointerEmptyFields(t *testing.T) {
	pointer := Pointer{
		Size: -40,
	}
	_, err := WritePointer(pointer)
	if err == nil {
		t.Error("Must be an error for empty bucket name")
	}
	pointer.BlobBucket = "bucket"
	_, err = WritePointer(pointer)
	if err == nil {
		t.Error("Must be an error for empty blob key")
	}
	pointer.BlobKey = "blobs/hash"
	_, err = WritePointer(pointer)
	if err == nil {
		t.Error("Must be an error for empty hash")
	}
	pointer.Hash = "hash"
	_, err = WritePointer(pointer)
	if err == nil {
		t.Error("Must be an error for negative size")
	}
}

func TestReadPointerBasic(t *testing.T) {
	pointer := `{"blob_bucket":"bucket", "blob_key":"blobs/hash", "hash":"hash", "size":100, "content_type":".json"}`
	resPointer, err := ReadPointer(strings.NewReader(pointer))
	if err != nil {
		t.Fatal(err)
	}
	expPointer := Pointer{
		BlobBucket:  "bucket",
		BlobKey:     "blobs/hash",
		Hash:        "hash",
		Size:        100,
		ContentType: ".json",
	}
	isEqual := cmp.Equal(resPointer, &expPointer)
	if !isEqual {
		t.Error("WriteJSON basic test failed")
	}
}
