package database

import "go-Redis/redis/protocol"

type ExecFunc func(db *DB, cmdLine [][]byte) protocol.Reply
type UndoFunc func(db *DB, cmdLine [][]byte) [][][]byte
type Command struct {
	executor ExecFunc
	undo     UndoFunc
	Arity    int
}

var Router = map[string]*Command{
	"PING":      {execPing, nil, 0},
	"SET":       {execSet, undoSet, 2},
	"GET":       {execGet, nil, 1},
	"DEL":       {execDel, undoDel, -1},
	"EXISTS":    {execExists, nil, -1},
	"KEYS":      {execKeys, nil, 1},
	"MSET":      {execMSET, undoMSet, -1},
	"TYPE":      {execType, nil, 1},
	"MGET":      {execMGET, nil, -1},
	"EXPIRE":    {execExpire, undoExpire, 2},
	"TTL":       {execTTL, nil, 1},
	"PERSIST":   {execPersist, undoPersist, 1},
	"HSET":      {execHset, nil, 3},
	"HGET":      {execHget, nil, 2},
	"HEXISTS":   {execHexists, nil, 2},
	"HDEL":      {execHdel, nil, -2},
	"LPUSH":     {execLPush, nil, -2},
	"LPOP":      {execLPop, nil, 1},
	"LRANGE":    {execLRange, nil, 3},
	"RPUSH":     {execRPush, nil, -2},
	"ZADD":      {execZAdd, nil, 3},
	"ZSCORE":    {execZScore, nil, 2},
	"ZRANGE":    {execZRange, nil, 3},
	"SADD":      {execSAdd, nil, -2},
	"SREM":      {execSRem, nil, -2},
	"SISMEMBER": {execSIsmember, nil, 2},
	"INCR":      {execIncr, undoIncr, 1},
	"DECR":      {execDecr, undoDecr, 1},
	"DBSIZE":    {execDBSize, nil, 0},
	"PEXPIREAT": {execPExpireAt, undoPExpireAt, 2},
	"WAIT":      {nil, nil, 2},
}
