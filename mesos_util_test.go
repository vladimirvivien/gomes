package gomes

import (
	"testing"
)

func TestNewFrameworkID(t *testing.T) {
	id := NewFrameworkID("test-id")
	if id == nil {
		t.Fatal("Not creating protobuf object.")
	}
	if id.GetValue() != "test-id" {
		t.Fatal("Protobuf object not returning expected value.")
	}
}
