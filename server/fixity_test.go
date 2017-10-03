package server

import (
	"testing"
)

func TestValidtion(t *testing.T) {

	validStats := []string{"ok", "scheduled", "error", "mismatches"}

	// test for https://github.com/ndlib/bendo/issues/164
	t.Log("testing fixity validation routines")

	for _, status := range validStats {
		statusValue, statusErr := statusValidate(status)
		if statusErr != nil {
			t.Fatalf("Received %#v, expected %#v", status, statusValue)
			return
		}
	}
}
