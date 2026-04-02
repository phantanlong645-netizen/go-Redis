package aof

import (
	"path/filepath"
	"testing"

	"go-Redis/database"
)

func TestRewriteStringMultiDB(t *testing.T) {
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

	db0 := dbSet.GetDB(0)
	db1 := dbSet.GetDB(1)

	db0.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("redis")})
	db0.Exec([][]byte{[]byte("SET"), []byte("lang"), []byte("go")})
	db1.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("db1")})

	if err := p.Rewrite(dbSet); err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

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

func TestRewriteKeepsIncrementalCommands(t *testing.T) {
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

	db := dbSet.GetDB(0)

	// 多放一些 string key，尽量拉长 rewrite 窗口。
	for i := 0; i < 1000; i++ {
		key := []byte("k" + string(rune('a'+(i%26))) + string(rune('A'+(i%26))) + string(rune('0'+(i%10))))
		value := []byte("v" + string(rune('0'+(i%10))))
		db.Exec([][]byte{[]byte("SET"), key, value})
	}
	db.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("old")})

	done := make(chan error, 1)
	go func() {
		done <- p.Rewrite(dbSet)
	}()

	// rewrite 期间模拟一条新的成功写命令：
	// 先更新内存，再把同一条命令写进 AOF 追加链路。
	incrementalCmd := [][]byte{[]byte("SET"), []byte("name"), []byte("new")}
	db.Exec(incrementalCmd)
	if err := p.SaveCmdLine(0, incrementalCmd); err != nil {
		t.Fatalf("write incremental cmd failed: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	restored := database.NewDBSet()
	if err := p.Load(restored); err != nil {
		t.Fatalf("load rewritten aof failed: %v", err)
	}

	if got := string(restored.GetDB(0).Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$3\r\nnew\r\n" {
		t.Fatalf("incremental command should be preserved, got: %q", got)
	}
}
