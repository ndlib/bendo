// +build integration

package server

import (
	"os"
	"testing"
	"time"

	"github.com/ndlib/bendo/items"
)

var dialmysql string

func init() {
	// check for external config for mysql, default to /test
	dialmysql = os.Getenv("MYSQL_CONNECTION")
	if dialmysql == "" {
		dialmysql = "/test"
	}
}

func resetMysql(mc *MsqlCache) {
	mc.db.Exec("DROP TABLE fixity")
	mc.db.Exec("DROP TABLE items")
	mc.db.Exec("DROP TABLE migration_version")
	mc.db.Exec("DROP TABLE blobs")
	mc.db.Exec("DROP TABLE slots")
	mc.db.Exec("DROP TABLE versions")
}

func TestMySQLItemCache(t *testing.T) {
	mc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}

	testItem := &items.Item{
		Versions: []*items.Version{
			{SaveDate: time.Now()},
		},
	}
	mc.Set("qwe", testItem)
	result := mc.Lookup("qwe")
	// should do deep equal. we are fudging it for now
	if result == nil {
		t.Errorf("Received nil, expected non-nil")
	}
	resetMysql(mc)
}

func TestMySQLFixity(t *testing.T) {
	mc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runFixitySequence(t, mc)
	resetMysql(mc)
}

func TestMySQLSearchFixity(t *testing.T) {
	mc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runSearchFixity(t, mc)
	resetMysql(mc)
}

func TestMySQLDelete(t *testing.T) {
	mc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runDeleteFixity(t, mc)
	resetMysql(mc)
}
