package std

import (
	"context"
	"go-Redis/aof"
	"go-Redis/database"
	"go-Redis/redis/parser"
	"go-Redis/redis/protocol"
	"net"
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
	persister, err := aof.NewPersister(filename, aof.FsyncAlways)
	if err != nil {
		panic(err)
	}
	if err = persister.Load(dbSet); err != nil {
		panic(err)
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

		cmd := strings.ToUpper(string(payload.Data[0]))
		if cmd == "SELECT" {
			if len(payload.Data) != 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for SELECT\r\n"))
				continue
			}
			index, err := strconv.Atoi(string(payload.Data[1]))
			if err != nil {
				_, _ = conn.Write([]byte("-ERR invalid DB index\r\n"))
				continue
			}
			if h.dbSet.GetDB(index) == nil {
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			selectedDB = index
			if h.persister != nil {
				_ = h.persister.SaveCmdLine(index, payload.Data)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
			continue
		}
		if cmd == "BGREWRITEAOF" {
			if len(payload.Data) != 1 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for SELECT\r\n"))
				continue
			}
			if h.persister == nil {
				_, _ = conn.Write([]byte("-ERR AOF is not enabled\r\n"))
			}
			go h.persister.Rewrite(h.dbSet)
			_, _ = conn.Write([]byte("+OK\r\n"))
			continue
		}

		db := h.dbSet.GetDB(selectedDB)
		reply := db.Exec(payload.Data)
		if h.persister != nil && isWriteCommand(cmd) && !isErrorReply(reply) {
			_ = h.persister.SaveCmdLine(selectedDB, payload.Data)
		}
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
		"EXPIRE", "PERSIST",
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
