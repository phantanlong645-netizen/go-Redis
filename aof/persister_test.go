package aof

import (
	"os"
	"path/filepath"
	"testing"

	"go-Redis/database"
)

func TestMarshalCmd(t *testing.T) {
	cmdLine := [][]byte{
		[]byte("SET"),
		[]byte("name"),
		[]byte("redis"),
	}

	got := string(marshalCmd(cmdLine))
	want := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nredis\r\n"

	t.Logf("marshal result: %q", got)
	if got != want {
		t.Fatalf("unexpected marshal result: got %q want %q", got, want)
	}
}

func TestPersisterWriteAndLoad(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "appendonly.aof")

	dbSet := database.NewDBSet()
	p, err := NewPersister(dbSet, filename, FsyncAlways)
	if err != nil {
		t.Fatalf("new persister failed: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	cmds := [][][]byte{
		{[]byte("SET"), []byte("name"), []byte("redis")},
		{[]byte("HSET"), []byte("user"), []byte("age"), []byte("18")},
		{[]byte("LPUSH"), []byte("mylist"), []byte("a")},
	}

	for _, cmd := range cmds {
		if err := p.SaveCmdLine(0, cmd); err != nil {
			t.Fatalf("write cmd failed: %v", err)
		}
	}

	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof file failed: %v", err)
	}
	t.Logf("aof file contents:\n%s", string(raw))

	if err := p.Load(dbSet); err != nil {
		t.Fatalf("load aof failed: %v", err)
	}
	db := dbSet.GetDB(0)

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$5\r\nredis\r\n" {
		t.Fatalf("unexpected restored string value: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("HGET"), []byte("user"), []byte("age")}).ToBytes()); got != "$2\r\n18\r\n" {
		t.Fatalf("unexpected restored hash value: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("LRANGE"), []byte("mylist"), []byte("0"), []byte("-1")}).ToBytes()); got != "*1\r\n$1\r\na\r\n" {
		t.Fatalf("unexpected restored list value: %q", got)
	}
}
func TestRewriteStringMultiDBReload(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "appendonly.aof")

	dbSet := database.NewDBSet()
	p, err := NewPersister(dbSet, filename, FsyncAlways)
	if err != nil {
		t.Fatalf("new persister failed: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// 先构造当前内存状态，而不是依赖旧 AOF 重放。
	db0 := dbSet.GetDB(0)
	db1 := dbSet.GetDB(1)

	db0.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("redis")})
	db0.Exec([][]byte{[]byte("SET"), []byte("lang"), []byte("go")})
	db1.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("db1")})

	// 执行 rewrite，把当前状态导出成新的 AOF 文件。
	if err := p.Rewrite(dbSet); err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	// 用一个全新的 DBSet 重新加载 rewrite 后的 AOF，验证恢复结果。
	restored := database.NewDBSet()
	if err := p.Load(restored); err != nil {
		t.Fatalf("load rewritten aof failed: %v", err)
	}

	rdb0 := restored.GetDB(0)
	rdb1 := restored.GetDB(1)

	if got := string(rdb0.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$5\r\nredis\r\n" {
		t.Fatalf("unexpected restored db0 name value: %q", got)
	}

	if got := string(rdb0.Exec([][]byte{[]byte("GET"), []byte("lang")}).ToBytes()); got != "$2\r\ngo\r\n" {
		t.Fatalf("unexpected restored db0 lang value: %q", got)
	}

	if got := string(rdb1.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$3\r\ndb1\r\n" {
		t.Fatalf("unexpected restored db1 name value: %q", got)
	}

	if got := string(rdb1.Exec([][]byte{[]byte("GET"), []byte("lang")}).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("unexpected restored db1 lang value: %q", got)
	}
}
func TestPersisterWriteAndLoadMultiDB(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "appendonly.aof")

	dbSet := database.NewDBSet()
	p, err := NewPersister(dbSet, filename, FsyncAlways)
	if err != nil {
		t.Fatalf("new persister failed: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	entries := []struct {
		dbIndex int
		cmdLine [][]byte
	}{
		{dbIndex: 0, cmdLine: [][]byte{[]byte("SET"), []byte("name"), []byte("db0")}},
		{dbIndex: 1, cmdLine: [][]byte{[]byte("SET"), []byte("name"), []byte("db1")}},
		{dbIndex: 0, cmdLine: [][]byte{[]byte("SET"), []byte("lang"), []byte("go")}},
	}

	for _, entry := range entries {
		if err := p.SaveCmdLine(entry.dbIndex, entry.cmdLine); err != nil {
			t.Fatalf("write cmd failed: %v", err)
		}
	}

	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof file failed: %v", err)
	}
	t.Logf("multi db aof file contents:\n%s", string(raw))

	if err := p.Load(dbSet); err != nil {
		t.Fatalf("load multi db aof failed: %v", err)
	}

	db0 := dbSet.GetDB(0)
	db1 := dbSet.GetDB(1)

	if got := string(db0.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$3\r\ndb0\r\n" {
		t.Fatalf("unexpected restored db0 name value: %q", got)
	}

	if got := string(db0.Exec([][]byte{[]byte("GET"), []byte("lang")}).ToBytes()); got != "$2\r\ngo\r\n" {
		t.Fatalf("unexpected restored db0 lang value: %q", got)
	}

	if got := string(db1.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$3\r\ndb1\r\n" {
		t.Fatalf("unexpected restored db1 name value: %q", got)
	}

	if got := string(db1.Exec([][]byte{[]byte("GET"), []byte("lang")}).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("unexpected restored db1 lang value: %q", got)
	}
}
