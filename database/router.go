package database

import "go-Redis/redis/protocol"

type ExecFunc func(db *DB, cmdLine [][]byte) protocol.Reply

var Router = map[string]ExecFunc{
	"PING": execPing,
	"SET":  execSet,
	"GET":  execGet,
	"DEL":  execDel,
}
