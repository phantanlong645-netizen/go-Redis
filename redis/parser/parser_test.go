package parser

import (
	"strings"
	"testing"
)

func TestParseStreamInline(t *testing.T) {
	ch := ParseStream(strings.NewReader("PING\r\n"))

	payload, ok := <-ch
	if !ok {
		t.Fatal("expected payload")
	}
	if payload.Err != nil {
		t.Fatalf("unexpected error: %v", payload.Err)
	}
	if len(payload.Data) != 1 {
		t.Fatalf("unexpected arg count: %d", len(payload.Data))
	}
	if string(payload.Data[0]) != "PING" {
		t.Fatalf("unexpected command: %q", payload.Data[0])
	}
}

func TestParseStreamRESP(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nredis\r\n"
	ch := ParseStream(strings.NewReader(input))

	payload, ok := <-ch
	if !ok {
		t.Fatal("expected payload")
	}
	if payload.Err != nil {
		t.Fatalf("unexpected error: %v", payload.Err)
	}
	if len(payload.Data) != 3 {
		t.Fatalf("unexpected arg count: %d", len(payload.Data))
	}
	if string(payload.Data[0]) != "SET" {
		t.Fatalf("unexpected cmd: %s", payload.Data[0])
	}
	if string(payload.Data[1]) != "name" {
		t.Fatalf("unexpected key: %s", payload.Data[1])
	}
	if string(payload.Data[2]) != "redis" {
		t.Fatalf("unexpected value: %s", payload.Data[2])
	}
}
