package std

import (
	"context"
	"go-Redis/aof"
	"go-Redis/database"
	"go-Redis/interface/redis"
	"go-Redis/pubsub"
	"go-Redis/redis/connection"
	"go-Redis/redis/parser"
	"go-Redis/redis/protocol"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	masterRole = int32(iota)
	slaveRole
)

type Handler struct {
	dbSet     *database.DBSet
	persister *aof.Persister
	hub       *pubsub.Hub
	role      int32

	slaveMu    sync.Mutex
	slaves     []redis.Connection
	masterHost string
	masterPort string
	masterConn redis.Connection
	masterChan <-chan *parser.Payload
	masterCtx  context.Context
	cancel     context.CancelFunc
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
		dbSet:      dbSet,
		persister:  persister,
		hub:        pubsub.MakeHub(),
		role:       masterRole,
		slaves:     make([]redis.Connection, 0),
		masterConn: nil,
	}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {

	ch := parser.ParseStream(conn)
	client := connection.NewConn(conn)
	defer func() {
		h.afterClientClose(client)
		_ = client.Close()
	}()
	for payload := range ch {
		if payload.Err != nil {
			return
		}
		if len(payload.Data) == 0 {
			continue
		}
		reply := h.Exec(client, payload.Data)
		if reply != nil {
			_, _ = client.Write(reply.ToBytes())
		}

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
func (h *Handler) Exec(c redis.Connection, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) == 0 {
		return protocol.NewErrReply("ERR empty command")
	}
	cmd := strings.ToUpper(string(cmdLine[0]))

	switch cmd {
	case "SELECT":
		return h.execSelect(c, cmdLine)
	case "BGREWRITEAOF":
		return h.execBGRewriteAOF(cmdLine)
	case "SAVE":
		return h.execSaveRDB(cmdLine)
	case "BGSAVE":
		return h.execBGSaveRDB(cmdLine)
	case "PUBLISH":
		return pubsub.Publish(h.hub, cmdLine[1:])
	case "SUBSCRIBE":
		return pubsub.Subscribe(h.hub, c, cmdLine[1:])
	case "UNSUBSCRIBE":
		return pubsub.UnSubscribe(h.hub, c, cmdLine[1:])
	case "SLAVEOF":
		return h.execSlaveOf(cmdLine[1:])
	case "REPLCONF":
		return h.execReplConf(c, cmdLine[1:])
	}
	if h.isSlaveRole() && isWriteCommand(cmd) && !c.IsMaster() {
		return protocol.NewErrReply("READONLY You can't write against a read only slave.")
	}
	db := h.dbSet.GetDB(c.GetDBIndex())
	if db == nil {
		return protocol.NewErrReply("ERR DB index is out of range")
	}
	reply := db.Exec(cmdLine)
	if isWriteCommand(cmd) && !isErrorReply(reply) {
		if h.persister != nil {
			_ = h.persister.SaveCmdLine(c.GetDBIndex(), makeAofCmdLine(db, cmdLine))
		}
		if h.isMasterRole() {
			h.propagateToSlaves(cmdLine)
		}

	}
	return reply
}
func (h *Handler) execReplConf(c redis.Connection, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for REPLCONF")
	}
	key := strings.ToUpper(string(args[0]))
	switch key {
	case "LISTENING-PORT":
		if !c.IsSlave() {
			c.SetSlave()
			h.addSlave(c)
		}
		return protocol.NewStatusReply("OK")
	default:
		return protocol.NewStatusReply("OK")
	}

}
func (h *Handler) execSelect(c redis.Connection, cmdLine [][]byte) protocol.Reply {

	if len(cmdLine) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for SELECT")
	}
	index, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid DB index")
	}
	if h.dbSet.GetDB(index) == nil {
		return protocol.NewErrReply("ERR DB index is out of range")

	}
	c.SelectDB(index)
	if h.persister != nil {
		_ = h.persister.SaveCmdLine(index, cmdLine)
	}
	return protocol.NewStatusReply("OK")
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
func (h *Handler) execSlaveOf(args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for SLAVEOF")
	}
	host := string(args[0])
	port := string(args[1])
	if strings.ToUpper(host) == "NO" && strings.ToUpper(port) == "ONE" {
		atomic.StoreInt32(&h.role, masterRole)
		h.masterHost = ""
		h.masterPort = ""
		if h.cancel != nil {
			h.cancel()
			h.cancel = nil
		}
		h.masterChan = nil
		h.masterCtx = nil
		if h.masterConn != nil {
			_ = h.masterConn.Close()
			h.masterConn = nil
		}
		return protocol.NewStatusReply("OK")
	}
	atomic.StoreInt32(&h.role, slaveRole)
	h.masterHost = host
	h.masterPort = port
	if err := h.connectMaster(); err != nil {
		atomic.StoreInt32(&h.role, masterRole)
		h.masterHost = ""
		h.masterPort = ""
		h.masterConn = nil
		return protocol.NewErrReply("ERR connect to master failed")
	}

	return protocol.NewStatusReply("OK")

}
func (h *Handler) isMasterRole() bool {
	return atomic.LoadInt32(&h.role) == masterRole
}
func (h *Handler) isSlaveRole() bool {
	return atomic.LoadInt32(&h.role) == slaveRole
}
func (h *Handler) addSlave(c redis.Connection) {
	h.slaveMu.Lock()
	defer h.slaveMu.Unlock()
	h.slaves = append(h.slaves, c)

}
func (h *Handler) getSlaves() []redis.Connection {
	h.slaveMu.Lock()
	defer h.slaveMu.Unlock()
	res := make([]redis.Connection, len(h.slaves))
	copy(res, h.slaves)
	return res

}
func (h *Handler) connectMaster() error {
	address := net.JoinHostPort(h.masterHost, h.masterPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	masterConn := connection.NewConn(conn)
	masterConn.SetMaster()
	masterChan := parser.ParseStream(conn)
	ctx, cancel := context.WithCancel(context.Background())
	h.masterChan = masterChan
	h.masterCtx = ctx
	h.cancel = cancel

	replConf := protocol.NewMultiBulkReply([][]byte{
		[]byte("REPLCONF"),
		[]byte("listening-port"),
		[]byte("6379"),
	})
	if _, err := conn.Write(replConf.ToBytes()); err != nil {
		_ = conn.Close()
		return err
	}

	h.masterConn = masterConn
	go h.receiveMasterCommands()
	return nil
}
func (h *Handler) propagateToSlaves(cmdLine [][]byte) {
	reply := protocol.NewMultiBulkReply(cmdLine)
	data := reply.ToBytes()
	for _, slave := range h.getSlaves() {
		_, _ = slave.Write(data)
	}

}
func (h *Handler) receiveMasterCommands() {
	for {
		select {
		case payload, ok := <-h.masterChan:
			if !ok {
				return
			}
			if payload.Err != nil {
				return
			}
			if len(payload.Data) == 0 {
				continue
			}
			_ = h.Exec(h.masterConn, payload.Data)
		case <-h.masterCtx.Done():
			return
		}
	}
}
func (h *Handler) afterClientClose(c redis.Connection) {
	pubsub.UnSubscribe(h.hub, c, nil)
	if c.IsSlave() {
		h.removeSlave(c)
	}
}
func (h *Handler) removeSlave(c redis.Connection) {
	h.slaveMu.Lock()
	defer h.slaveMu.Unlock()
	filterd := make([]redis.Connection, 0, len(h.slaves))
	for _, slave := range h.slaves {
		if slave != c {
			filterd = append(filterd, slave)
		}
	}
	h.slaves = filterd

}
