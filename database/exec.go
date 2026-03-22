package database

import (
	"go-Redis/redis/protocol"
	"strings"
)

func (db *DB) Exec(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) == 0 {
		return protocol.NewErrReply("ERR empty command")
	}
	cmd := strings.ToUpper(string(cmdLine[0]))
	args := cmdLine[1:]
	f, ok := Router[cmd]
	if !ok {
		return protocol.NewErrReply("ERR unknown command")
	}
	return f(db, args)
}
