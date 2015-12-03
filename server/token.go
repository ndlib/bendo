package server

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strings"
)

// A TokenDecoder validates and decodes user tokens passed into the web API. If
// the given token is not valid, for whatever reason, the user "" with a role of
// RoleUnknown is returned. An error is returned only if there is some kind of error doing
// the lookup and the ultimate status of the token is unknown.
type TokenDecoder interface {
	TokenDecode(token string) (user string, role Role, err error)
}

type Role int

const (
	RoleUnknown Role = iota
	RoleMDOnly
	RoleRead
	RoleWrite
	RoleAdmin
)

func atoRole(s string) Role {
	switch strings.ToLower(s) {
	case "mdonly":
		return RoleMDOnly
	case "read":
		return RoleRead
	case "write":
		return RoleWrite
	case "admin":
		return RoleAdmin
	default:
		return RoleUnknown
	}
}

// NewNobodyDecoder creates a TokenDecoder that for every possible token
// returns a user named "nobody" with the Admin role.
func NewNobodyDecoder() TokenDecoder {
	return new(nobodyDecoder)
}

type nobodyDecoder struct{}

func (_ nobodyDecoder) TokenDecode(token string) (user string, role Role, err error) {
	return "nobody", RoleAdmin, nil
}

// A ListDecoder is backed by a predefined list of users, which are read from r upon creation.
// The reader r should consist of a sequence of user entries, separated by newlines.
// Each entry has the form:
//
//     <user name>  <role>  <token>
//
// The fields are delineated by whitespace (spaces or tabs).
// This decoder does not permit spaces in either the user
// name or the token. The role is one of "MDOnly", "Read",
// "Write", "Admin" (case insensitive). Empty lines and lines beginning with a
// hash '#' are skipped.
func NewListDecoder(r io.Reader) (TokenDecoder, error) {
	users, err := parseListFile(r)
	if err != nil {
		return nil, err
	}
	sort.Sort(byToken(users))
	return listDecoder{users}, nil
}

// NewListDecoderFile is a convenience function that reads the contents of
// the given file into a ListDecoder. The file should have the same format
// that NewListDecoder expects.
func NewListDecoderFile(fname string) (TokenDecoder, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return NewListDecoder(f)
}

// NewListDecoderString is a convenience function that passes the given string
// into a ListDecoder. The format of the string is the same as that expected
// by NewListDecoder.
func NewListDecoderString(data string) (TokenDecoder, error) {
	return NewListDecoder(strings.NewReader(data))
}

func parseListFile(r io.Reader) ([]userEntry, error) {
	var result []userEntry
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// split on whitespace
		pieces := strings.Fields(scanner.Text())
		// skip blank lines or lines beginning with a '#'
		if len(pieces) == 0 || pieces[0][0] == '#' {
			continue
		}
		if len(pieces) != 3 {
			// wrong number of columns
			continue
		}
		result = append(result, userEntry{
			token: pieces[2],
			user:  pieces[0],
			role:  atoRole(pieces[1]),
		})
	}
	return result, scanner.Err()
}

type listDecoder struct {
	data []userEntry
}

type byToken []userEntry

func (ue byToken) Len() int           { return len(ue) }
func (ue byToken) Less(i, j int) bool { return ue[i].token < ue[j].token }
func (ue byToken) Swap(i, j int)      { ue[i], ue[j] = ue[j], ue[i] }

type userEntry struct {
	token string
	user  string
	role  Role
}

func (ld listDecoder) TokenDecode(token string) (string, Role, error) {
	users := ld.data
	i := sort.Search(len(users), func(i int) bool { return users[i].token >= token })
	if i < len(users) && users[i].token == token {
		return users[i].user, users[i].role, nil
	} else {
		return "", RoleUnknown, nil
	}
}
