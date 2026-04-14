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
	cmdObj, ok := Router[cmd]
	if !ok {
		return protocol.NewErrReply("ERR unknown command")
	}
	if cmdObj.Arity >= 0 {
		if cmdObj.Arity != len(args) {
			return protocol.NewErrReply("ERR wrong number of arguments for " + cmd)
		}
	} else {
		if len(args) < -cmdObj.Arity {
			return protocol.NewErrReply("ERR wrong number of arguments for " + cmd)
		}
	}
	return cmdObj.executor(db, args)
}
