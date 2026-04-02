package aof

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go-Redis/database"

	rdbcore "github.com/hdt3213/rdb/core"
	rdbmodel "github.com/hdt3213/rdb/model"
)

func TestGenerateRDB(t *testing.T) {
	dir := t.TempDir()
	aofFilename := filepath.Join(dir, "appendonly.aof")
	rdbFilename := filepath.Join("dump.rdb")
	t.Logf("rdb file: %s", rdbFilename)

	dbSet := database.NewDBSet()
	p, err := NewPersister(dbSet, aofFilename, FsyncAlways)
	if err != nil {
		t.Fatalf("new persister failed: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	db0 := dbSet.GetDB(0)
	db1 := dbSet.GetDB(1)
	if db0 == nil || db1 == nil {
		t.Fatal("expected db0 and db1 to exist")
	}

	if got := string(db0.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("redis")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected db0 set reply: %q", got)
	}
	if got := string(db1.Exec([][]byte{[]byte("SET"), []byte("lang"), []byte("go")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected db1 set reply: %q", got)
	}

	expireAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	if got := string(db0.Exec([][]byte{
		[]byte("PEXPIREAT"),
		[]byte("name"),
		[]byte(timeToMillis(expireAt)),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected pexpireat reply: %q", got)
	}

	if err := p.GenerateRDB(rdbFilename); err != nil {
		t.Fatalf("generate rdb failed: %v", err)
	}

	info, err := os.Stat(rdbFilename)
	if err != nil {
		t.Fatalf("stat rdb file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("generated rdb file should not be empty")
	}

	file, err := os.Open(rdbFilename)
	if err != nil {
		t.Fatalf("open rdb file failed: %v", err)
	}
	defer file.Close()

	decoder := rdbcore.NewDecoder(file)
	got := make(map[string]*rdbmodel.StringObject)
	err = decoder.Parse(func(object rdbmodel.RedisObject) bool {
		strObj, ok := object.(*rdbmodel.StringObject)
		if !ok {
			return true
		}
		got[rdbKey(strObj.GetDBIndex(), strObj.GetKey())] = strObj
		return true
	})
	if err != nil {
		t.Fatalf("parse generated rdb failed: %v", err)
	}

	nameObj := got[rdbKey(0, "name")]
	if nameObj == nil {
		t.Fatal("db0 string key 'name' not found in generated rdb")
	}
	if string(nameObj.Value) != "redis" {
		t.Fatalf("unexpected db0 name value: %q", string(nameObj.Value))
	}
	if nameObj.GetExpiration() == nil {
		t.Fatal("db0 key 'name' should carry expiration in generated rdb")
	}
	if nameObj.GetExpiration().UnixMilli() != expireAt.UnixMilli() {
		t.Fatalf("unexpected db0 name expiration: got %d want %d", nameObj.GetExpiration().UnixMilli(), expireAt.UnixMilli())
	}

	langObj := got[rdbKey(1, "lang")]
	if langObj == nil {
		t.Fatal("db1 string key 'lang' not found in generated rdb")
	}
	if string(langObj.Value) != "go" {
		t.Fatalf("unexpected db1 lang value: %q", string(langObj.Value))
	}
	if langObj.GetExpiration() != nil {
		t.Fatal("db1 key 'lang' should not carry expiration in generated rdb")
	}
}

func rdbKey(dbIndex int, key string) string {
	return timeToMillisInt(int64(dbIndex)) + "/" + key
}

func timeToMillis(t time.Time) string {
	return timeToMillisInt(t.UnixMilli())
}

func timeToMillisInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
