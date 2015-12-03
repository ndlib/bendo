package server

import (
	"testing"
)

func TestAtoRole(t *testing.T) {
	var table = []struct {
		input  string
		output Role
	}{
		{"MDOnly", RoleMDOnly},
		{"mdonly", RoleMDOnly},
		{"read", RoleRead},
		{"Read", RoleRead},
		{"Write", RoleWrite},
		{"write", RoleWrite},
		{"admin", RoleAdmin},
		{"Admin", RoleAdmin},
		{"other", RoleUnknown},
	}

	for _, row := range table {
		result := atoRole(row.input)
		if result != row.output {
			t.Errorf("For %v received %v, expected %v", row.input, result, row.output)
		}
	}
}
