package std

import (
	"context"
	"go-Redis/aof"
	"go-Redis/database"
	"go-Redis/redis/parser"
	"go-Redis/redis/protocol"
	"net"
	"os"
	"strconv"
	"strings"
)

type Handler struct {
	dbSet     *database.DBSet
	persister *aof.Persister
}

func NewHandler() *Handler {
	return NewHandlerWithAOF("appendonly.aof")

}
func NewHandlerWithAOF(filename string) *Handler {
	dbSet := database.NewDBSet()
	_, statErr := os.Stat(filename)
	aofExists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		panic(statErr)
	}
	persister, err := aof.NewPersister(dbSet, filename, aof.FsyncAlways)
	if err != nil {
		panic(err)
	}
	if aofExists {
		if err = persister.Load(dbSet); err != nil {
			panic(err)
		}
	} else {
		if _, err := os.Stat("dump.rdb"); err == nil {
			if err := aof.LoadRDBFile(dbSet, "dump.rdb"); err != nil {
				panic(err)
			}
		} else if !os.IsNotExist(err) {
			panic(err)
		}
	}
	return &Handler{
		dbSet:     dbSet,
		persister: persister,
	}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	ch := parser.ParseStream(conn)
	selectedDB := 0
	for payload := range ch {
		if payload.Err != nil {
			return
		}
		if len(payload.Data) == 0 {
			continue
		}
		reply, newDB := h.Exec(selectedDB, payload.Data)
		selectedDB = newDB
		_, _ = conn.Write(reply.ToBytes())

	}
}
func (h *Handler) Close() {
	if h.persister != nil {
		_ = h.persister.Close()
	}
}

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "MSET", "DEL",
		"EXPIRE", "PERSIST", "PEXPIREAT",
		"HSET", "HDEL",
		"LPUSH", "RPUSH", "LPOP",
		"SADD", "SREM",
		"ZADD",
		"INCR", "DECR":
		return true
	default:
		return false
	}
}
func isErrorReply(reply protocol.Reply) bool {
	_, ok := reply.(*protocol.ErrReply)
	return ok
}
func makeAofCmdLine(db *database.DB, cmdLine [][]byte) [][]byte {
	if len(cmdLine) == 0 {
		return nil
	}
	cmd := strings.ToUpper(string(cmdLine[0]))
	if cmd != "EXPIRE" || len(cmdLine) != 3 || db == nil {
		return cmdLine
	}
	key := string(cmdLine[1])
	expireAt, ok := db.TTL[key]
	if !ok {
		return cmdLine
	}
	return [][]byte{
		[]byte("PEXPIREAT"),
		[]byte(key),
		[]byte(strconv.FormatInt(expireAt.UnixMilli(), 10)),
	}
}
func (h *Handler) Exec(selectedDB int, cmdLine [][]byte) (protocol.Reply, int) {
	if len(cmdLine) == 0 {
		return protocol.NewErrReply("ERR empty command"), selectedDB
	}
	cmd := strings.ToUpper(string(cmdLine[0]))

	switch cmd {
	case "SELECT":
		return h.execSelect(selectedDB, cmdLine)
	case "BGREWRITEAOF":
		return h.execBGRewriteAOF(cmdLine), selectedDB
	case "SAVE":
		return h.execSaveRDB(cmdLine), selectedDB
	case "BGSAVE":
		return h.execBGSaveRDB(cmdLine), selectedDB
	}
	db := h.dbSet.GetDB(selectedDB)
	if db == nil {
		return protocol.NewErrReply("ERR DB index is out of range"), selectedDB
	}
	reply := db.Exec(cmdLine)
	if h.persister != nil && isWriteCommand(cmd) && !isErrorReply(reply) {
		_ = h.persister.SaveCmdLine(selectedDB, makeAofCmdLine(db, cmdLine))
	}
	return reply, selectedDB
}
func (h *Handler) execSelect(selectedDB int, cmdLine [][]byte) (protocol.Reply, int) {

	if len(cmdLine) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for SELECT"), selectedDB
	}
	index, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid DB index"), selectedDB
	}
	if h.dbSet.GetDB(index) == nil {
		return protocol.NewErrReply("ERR DB index is out of range"), selectedDB

	}
	selectedDB = index
	if h.persister != nil {
		_ = h.persister.SaveCmdLine(index, cmdLine)
	}
	return protocol.NewStatusReply("OK"), selectedDB
}
func (h *Handler) execBGRewriteAOF(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for BGREWRITEAOF")

	}
	if h.persister == nil {
		return protocol.NewErrReply("ERR AOF is not enabled")

	}
	go h.persister.Rewrite(h.dbSet)
	return protocol.NewStatusReply("OK")

}
func (h *Handler) execSaveRDB(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for SAVE")
	}
	if h.persister == nil {
		return protocol.NewErrReply("ERR AOF is not enabled")

	}
	if err := h.persister.GenerateRDB("dump.rdb"); err != nil {
		return protocol.NewErrReply("ERR save rdb failed")

	}
	return protocol.NewStatusReply("OK")

}
func (h *Handler) execBGSaveRDB(cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for BGSAVE")
	}
	if h.persister == nil {
		return protocol.NewErrReply("ERR AOF is not enabled")
	}
	go func() {
		_ = h.persister.GenerateRDB("dump.rdb")
	}()
	return protocol.NewStatusReply("Background saving started")

}
