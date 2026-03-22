package database

import "go-Redis/redis/protocol"

type ExecFunc func(db *DB, cmdLine [][]byte) protocol.Reply

type Command struct {
	executor ExecFunc
	arity    int
}

var Router = map[string]*Command{
	"PING": {execPing, 0},
	"SET":  {execSet, 2},
	"GET":  {execGet, 1},
	"DEL":  {execDel, -1},
}
