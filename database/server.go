package database

import (
	"go-Redis/redis/protocol"
	"strings"
)

func Exec(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) == 0 {
		return protocol.NewErrReply("ERR empty command")
	}
	cmd := strings.ToUpper(string(cmdLine[0]))
	if cmd == "PING" {
		return protocol.NewStatusReply("PONG")
	}
	return protocol.NewErrReply("ERR unknown command")
}
