package database

import (
	"go-Redis/redis/protocol"
	"strings"
)

type DB struct {
	Data map[string][]byte
}

func NewDB() *DB {
	return &DB{
		Data: make(map[string][]byte),
	}
}
func (db *DB) Exec(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) == 0 {
		return protocol.NewErrReply("ERR empty command")
	}
	cmd := strings.ToUpper(string(cmdLine[0]))
	if cmd == "PING" {
		return protocol.NewStatusReply("PONG")
	}
	if cmd == "SET" {
		if len(cmdLine) != 3 {
			return protocol.NewErrReply("ERR wrong number of arguments for SET")
		}
		db.Data[string(cmdLine[1])] = cmdLine[2]
		return protocol.NewStatusReply("OKahaha")
	}
	if cmd == "GET" {
		if len(cmdLine) != 2 {
			return protocol.NewErrReply("ERR wrong number of arguments for GET")
		}
		if value, ok := db.Data[string(cmdLine[1])]; !ok {
			return protocol.NewNullBulkReply()
		} else {
			return protocol.NewBulkReply(value)
		}
	}
	if cmd == "DEL" {
		if len(cmdLine) < 2 {
			return protocol.NewErrReply("ERR wrong number of arguments for DEL")
		}
		var deleteCount int64
		for _, arg := range cmdLine[1:] {
			key := string(arg)
			if _, ok := db.Data[key]; ok {
				delete(db.Data, key)
				deleteCount++
			}
		}
		return protocol.NewIntReply(deleteCount)
	}
	return protocol.NewErrReply("ERR unknown command")
}
