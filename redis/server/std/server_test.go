package std

import (
	"bufio"
	"context"
	"go-Redis/aof"
	"go-Redis/database"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSelectCommand(t *testing.T) {
	dir := t.TempDir()
	handler := NewHandlerWithAOF(filepath.Join(dir, "appendonly.aof"))
	defer handler.Close()
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Handle(context.Background(), serverConn)
	}()

	reader := bufio.NewReader(clientConn)

	assertCommandReply(t, clientConn, reader, "SET name redis\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "GET name\r\n", "$5\r\nredis\r\n")
	assertCommandReply(t, clientConn, reader, "SELECT 1\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "GET name\r\n", "$-1\r\n")
	assertCommandReply(t, clientConn, reader, "SET name db1\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "GET name\r\n", "$3\r\ndb1\r\n")
	assertCommandReply(t, clientConn, reader, "SELECT 0\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "GET name\r\n", "$5\r\nredis\r\n")

	_ = clientConn.Close()
	<-done
}
func TestBGRewriteAOFCommand(t *testing.T) {
	dir := t.TempDir()
	handler := NewHandlerWithAOF(filepath.Join(dir, "appendonly.aof"))
	defer handler.Close()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Handle(context.Background(), serverConn)
	}()

	reader := bufio.NewReader(clientConn)

	assertCommandReply(t, clientConn, reader, "SET name redis\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "BGREWRITEAOF\r\n", "+OK\r\n")

	_ = clientConn.Close()
	<-done
}

func TestBGSaveCommand(t *testing.T) {
	dir := t.TempDir()
	handler := NewHandlerWithAOF(filepath.Join(dir, "appendonly.aof"))
	defer handler.Close()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Handle(context.Background(), serverConn)
	}()

	reader := bufio.NewReader(clientConn)

	assertCommandReply(t, clientConn, reader, "SET name redis\r\n", "+OK\r\n")
	assertCommandReply(t, clientConn, reader, "BGSAVE\r\n", "+Background saving started\r\n")

	_ = clientConn.Close()
	<-done
}

func TestNewHandlerWithAOFLoadsRDBWhenAOFMissing(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory failed: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("change working directory failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
	})

	dbSet := database.NewDBSet()
	dbSet.GetDB(0).Data["name"] = &database.DataEntity{
		Type: "string",
		Data: []byte("redis"),
	}
	dbSet.GetDB(1).Data["lang"] = &database.DataEntity{
		Type: "string",
		Data: []byte("go"),
	}

	persister, err := aof.NewPersister(dbSet, filepath.Join(dir, "source.aof"), aof.FsyncAlways)
	if err != nil {
		t.Fatalf("create persister failed: %v", err)
	}
	defer persister.Close()

	if err := persister.GenerateRDB("dump.rdb"); err != nil {
		t.Fatalf("generate rdb failed: %v", err)
	}

	handler := NewHandlerWithAOF(filepath.Join(dir, "appendonly.aof"))
	defer handler.Close()

	entity := handler.dbSet.GetDB(0).Data["name"]
	if entity == nil {
		t.Fatal("db0 key name not loaded from rdb")
	}
	value, ok := entity.Data.([]byte)
	if !ok || string(value) != "redis" {
		t.Fatalf("unexpected db0 value: %#v", entity.Data)
	}

	entity = handler.dbSet.GetDB(1).Data["lang"]
	if entity == nil {
		t.Fatal("db1 key lang not loaded from rdb")
	}
	value, ok = entity.Data.([]byte)
	if !ok || string(value) != "go" {
		t.Fatalf("unexpected db1 value: %#v", entity.Data)
	}
}

func assertCommandReply(t *testing.T, conn net.Conn, reader *bufio.Reader, command string, expected string) {
	t.Helper()

	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline failed: %v", err)
	}
	if _, err := conn.Write([]byte(command)); err != nil {
		t.Fatalf("write command failed: %v", err)
	}

	reply := readRESPReply(t, reader)
	if reply != expected {
		t.Fatalf("unexpected reply for %q: got %q want %q", strings.TrimSpace(command), reply, expected)
	}
}

func readRESPReply(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	prefix, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read reply prefix failed: %v", err)
	}

	switch prefix {
	case '+', '-', ':':
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read line reply failed: %v", err)
		}
		return string(prefix) + line
	case '$':
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk length failed: %v", err)
		}
		reply := string(prefix) + line

		lengthText := strings.TrimSuffix(line, "\n")
		lengthText = strings.TrimSuffix(lengthText, "\r")
		length, err := strconv.Atoi(lengthText)
		if err != nil {
			t.Fatalf("invalid bulk length %q: %v", lengthText, err)
		}
		if length == -1 {
			return reply
		}

		data := make([]byte, length+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			t.Fatalf("read bulk body failed: %v", err)
		}
		return reply + string(data)
	case '*':
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read array length failed: %v", err)
		}
		reply := string(prefix) + line

		countText := strings.TrimSuffix(line, "\n")
		countText = strings.TrimSuffix(countText, "\r")
		count, err := strconv.Atoi(countText)
		if err != nil {
			t.Fatalf("invalid array length %q: %v", countText, err)
		}

		for i := 0; i < count; i++ {
			reply += readRESPReply(t, reader)
		}
		return reply
	default:
		t.Fatalf("unexpected reply prefix: %q", prefix)
		return ""
	}
}
