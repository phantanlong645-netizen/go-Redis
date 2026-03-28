package database

import "go-Redis/redis/protocol"

type ExecFunc func(db *DB, cmdLine [][]byte) protocol.Reply

type Command struct {
	executor ExecFunc
	arity    int
}

var Router = map[string]*Command{
	"PING":    {execPing, 0},
	"SET":     {execSet, 2},
	"GET":     {execGet, 1},
	"DEL":     {execDel, -1},
	"EXISTS":  {execExists, -1},
	"KEYS":    {execKeys, 1},
	"TYPE":    {execType, 1},
	"MGET":    {execMGET, -1},
	"MSET":    {execMSET, -1},
	"EXPIRE":  {execExpire, 2},
	"TTL":     {execTTL, 1},
	"PERSIST": {execPersist, 1},
	"HSET":    {execHset, 3},
	"HGET":    {execHget, 2},
	"HEXISTS": {execHexists, 2},
	"HDEL":    {execHdel, -2},
	"LPUSH":   {execLPush, -2},
	"LPOP":    {execLPop, 1},
	"LRANGE":  {execLRange, 3},
	"RPUSH":   {execRPush, -2},
}
