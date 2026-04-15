package std

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"go-Redis/aof"
	"go-Redis/database"
	"go-Redis/interface/redis"
	"go-Redis/pubsub"
	"go-Redis/redis/connection"
	"go-Redis/redis/parser"
	"go-Redis/redis/protocol"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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

	masterStatus    *masterStatus
	masterHost      string
	masterPort      string
	masterConn      redis.Connection
	masterChan      <-chan *parser.Payload
	masterCtx       context.Context
	cancel          context.CancelFunc
	slaveReplOffset int64
	slaveReplID     string
}

func NewHandler() *Handler {
	return NewHandlerWithAOF("appendonly.aof")

}

const replBacklogSize = 1024 * 1024

func NewHandlerWithAOF(filename string) *Handler {

	genReplId := func() string {
		buf := make([]byte, 20)
		if _, err := rand.Read(buf); err != nil {
			return "0000000000000000000000000000000000000000"
		}
		return hex.EncodeToString(buf)
	}

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
		dbSet:        dbSet,
		persister:    persister,
		hub:          pubsub.MakeHub(),
		role:         masterRole,
		masterStatus: initMasterStatus(genReplId(), replBacklogSize),
		masterConn:   nil,
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
	if c.InMultiState() && cmd != "MULTI" && cmd != "DISCARD" && cmd != "EXEC" {
		return h.enqueueCmdInMulti(c, cmdLine)
	}

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
	case "PSYNC":
		return h.execPSync(c, cmdLine[1:])
	case "WAIT":
		return h.execWait(cmdLine[1:])
	case "MULTI":
		return h.execMulti(c)
	case "DISCARD":
		return h.execDiscard(c)
	case "EXEC":
		return h.execExec(c)
	case "WATCH":
		return h.execWatch(c, cmdLine)
	case "UNWATCH":
		return h.execUnWatch(c, cmdLine)

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

func (h *Handler) execWait(args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for WAIT")
	}
	numReplicas, err := strconv.Atoi(string(args[0]))
	if err != nil || numReplicas < 0 {
		return protocol.NewErrReply("ERR wrong number of replicas for WAIT")
	}
	timeoutMS, err := strconv.Atoi(string(args[1]))
	if err != nil || timeoutMS < 0 {
		return protocol.NewErrReply("ERR wrong number of timeout for WAIT")
	}
	targetOffset := h.getReplOffset()
	acked := h.countOnlineSlavesAtOffset(targetOffset)
	if acked >= numReplicas {
		return protocol.NewIntReply(int64(acked))
	}
	//等于0 说明永远等待
	if timeoutMS == 0 {
		h.sendGetAckToSlaves()
		for {
			acked = h.countOnlineSlavesAtOffset(targetOffset)
			if acked >= numReplicas {
				return protocol.NewIntReply(int64(acked))
			}
			h.sendGetAckToSlaves()
			time.Sleep(10 * time.Millisecond) // 短暂休眠
		}
	}
	deadline := time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)
	lastGetAckSent := time.Now()
	for {
		acked = h.countOnlineSlavesAtOffset(targetOffset)
		if acked >= numReplicas {
			return protocol.NewIntReply(int64(acked))
		}
		if time.Now().After(deadline) {
			return protocol.NewIntReply(int64(acked))
		}
		if time.Since(lastGetAckSent) > 50*time.Millisecond {
			h.sendGetAckToSlaves()
			lastGetAckSent = time.Now()
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (h *Handler) sendGetAckToSlaves() {
	getAckcmd := protocol.NewMultiBulkReply([][]byte{
		[]byte("REPLCONF"),
		[]byte("GETACK"),
		[]byte("*"),
	})
	data := getAckcmd.ToBytes()
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	for slave := range h.masterStatus.onlineSlaves {
		go func(s *slaveClient) {
			_, err := s.conn.Write(data)
			if err != nil {
				log.Printf("error sending REPLCONF GETACK to slave: %v", err)
			}
		}(slave)
	}
}
func (h *Handler) execPSync(c redis.Connection, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for PSYNC")
	}
	if !c.IsSlave() {
		c.SetSlave()
	}
	replID := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.NewErrReply("ERR invalid PSYNC offset")

	}
	slave := h.getOrCreateSlave(c)
	data, currentOffset, ok := h.tryPartialResync(replID, offset)
	if ok {
		if _, err := slave.conn.Write([]byte("+CONTINUE\r\n")); err != nil {
			return protocol.NewErrReply("ERR send continue failed")
		}
		if len(data) > 0 {
			if _, err := slave.conn.Write(data); err != nil {
				return protocol.NewErrReply("ERR send partial resync data failed")
			}
		}
		h.setSlaveOnline(slave, currentOffset)
		return &protocol.NoReply{}

	}
	if err := h.sendFullResync(slave); err != nil {
		return protocol.NewErrReply("ERR send full resync failed")
	}
	return &protocol.NoReply{}
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
		}
		h.getOrCreateSlave(c)
		return protocol.NewStatusReply("OK")
	case "ACK":
		offset, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.NewErrReply("ERR invalid ACK offset")
		}
		h.masterStatus.mu.Lock()
		defer h.masterStatus.mu.Unlock()
		slave := h.masterStatus.slaveMap[c]
		if slave == nil {
			return &protocol.NoReply{}
		}
		if slave.state != slaveStateOnline {
			return &protocol.NoReply{}
		}
		slave.offset = offset
		slave.lastAckTime = time.Now()
		return &protocol.NoReply{}
	case "GETACK":
		currentOffset := h.getSlaveReplOffset()
		ackCmd := protocol.NewMultiBulkReply([][]byte{
			[]byte("REPLCONF"),
			[]byte("ACK"),
			[]byte(strconv.FormatInt(currentOffset, 10)),
		})
		_, err := c.Write(ackCmd.ToBytes())
		if err != nil {
			log.Printf("Slave error sending REPLCONF ACK to master: %v", err)
			// TODO: 考虑错误处理，例如关闭连接
		}
		return &protocol.NoReply{}

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
		h.slaveReplOffset = 0
		h.slaveReplID = ""
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
	h.slaveReplID = ""
	h.slaveReplOffset = 0
	if err := h.connectMaster(); err != nil {
		atomic.StoreInt32(&h.role, masterRole)
		h.masterHost = ""
		h.masterPort = ""
		h.masterConn = nil
		return protocol.NewErrReply("ERR connect to master failed")
	}

	return protocol.NewStatusReply("OK")

}

func (h *Handler) connectMaster() error {
	address := net.JoinHostPort(h.masterHost, h.masterPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	masterConn := connection.NewConn(conn)
	masterConn.SetMaster()
	reader := bufio.NewReader(conn)
	ctx, cancel := context.WithCancel(context.Background())

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
	line, err := reader.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return err
	}
	if line != "+OK\r\n" {
		_ = conn.Close()
		return fmt.Errorf("unexpected REPLCONF reply: %s", strings.TrimSpace(line))
	}
	replID := h.slaveReplID
	if replID == "" {
		replID = "?"
	}
	replOffset := h.getSlaveReplOffset()
	if replOffset <= 0 {
		replOffset = -1
	}
	psync := protocol.NewMultiBulkReply([][]byte{
		[]byte("PSYNC"),
		[]byte(replID),
		[]byte(strconv.FormatInt(replOffset, 10)),
	})
	if _, err := conn.Write(psync.ToBytes()); err != nil {
		_ = conn.Close()
		return err
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := h.handlePSyncReply(line, reader, masterConn); err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}
func (h *Handler) isMasterRole() bool {
	return atomic.LoadInt32(&h.role) == masterRole
}
func (h *Handler) isSlaveRole() bool {
	return atomic.LoadInt32(&h.role) == slaveRole
}

func (h *Handler) sendFullResync(slave *slaveClient) error {
	tmpDir, err := os.MkdirTemp("", "go-redis-fullresync-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	replId := h.getReplID()
	baseOffset := h.getReplOffset()
	rdbFile := filepath.Join(tmpDir, "dump.rdb")
	if err := h.persister.GenerateRDB(rdbFile); err != nil {
		return err
	}
	data, err := os.ReadFile(rdbFile)
	if err != nil {
		return err
	}
	if _, err := slave.conn.Write([]byte("+FULLRESYNC " + replId + " " + strconv.FormatInt(baseOffset, 10) + "\r\n")); err != nil {
		return err
	}
	if _, err := slave.conn.Write([]byte("$" + strconv.Itoa(len(data)) + "\r\n")); err != nil {
		return err
	}
	if _, err := slave.conn.Write(data); err != nil {
		return err
	}
	if _, err := slave.conn.Write([]byte("\r\n")); err != nil {
		return err
	}
	backlogData, latestOffset, ok := h.getBacklogSnapshotAfter(baseOffset)
	if !ok {
		return fmt.Errorf("replication backlog overwritten during full resync")
	}
	if len(backlogData) > 0 {
		if _, err := slave.conn.Write(backlogData); err != nil {
			return err
		}
	}
	h.setSlaveOnline(slave, latestOffset)
	return nil

}
func (h *Handler) handlePSyncReply(line string, reader *bufio.Reader, masterConn redis.Connection) error {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "+CONTINUE") {
		h.masterChan = parser.ParseStream(reader)
		h.masterConn = masterConn
		go h.receiveMasterCommands()
		return nil
	}
	if !strings.HasPrefix(line, "+FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC reply: %s", strings.TrimSpace(line))
	}
	parts := strings.Split(line[1:], " ")
	if len(parts) != 3 {
		return fmt.Errorf("unexpected PSYNC reply: %s", strings.TrimSpace(line))
	}
	replID := parts[1]
	replOffset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid FULLRESYNC offset: %s", parts[2])
	}
	h.slaveReplID = replID
	h.slaveReplOffset = replOffset
	line, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "$") {
		return fmt.Errorf("unexpected PSYNC reply: %s", strings.TrimSpace(line))
	}

	rdblen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || rdblen < 0 {
		return fmt.Errorf("invalid RDB length: %s", strings.TrimSpace(line))
	}

	rdbData := make([]byte, rdblen)
	if _, err := io.ReadFull(reader, rdbData); err != nil {
		return err
	}
	tail := make([]byte, 2)
	if _, err := io.ReadFull(reader, tail); err != nil {
		return err
	}
	if string(tail) != "\r\n" {
		return fmt.Errorf("invalid tail: %s", strings.TrimSpace(line))
	}
	tmpdir, err := os.MkdirTemp("", "go-redis-fullresync-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)
	rdbFile := filepath.Join(tmpdir, "dump.rdb")
	if err := os.WriteFile(rdbFile, rdbData, 0644); err != nil {
		return err
	}
	if err := aof.LoadRDBFile(h.dbSet, rdbFile); err != nil {
		return err
	}
	h.masterChan = parser.ParseStream(reader)
	h.masterConn = masterConn
	go h.receiveMasterCommands()
	return nil
}

func (h *Handler) reconnectMaster() {
	for {
		if !h.isSlaveRole() {
			return
		}
		if h.masterHost == "" || h.masterPort == "" {
			return
		}
		if err := h.connectMaster(); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}
func (h *Handler) handleMasterLinkDown() {
	if h.masterConn != nil {
		_ = h.masterConn.Close()
		h.masterConn = nil
	}
	h.masterChan = nil
	if h.isSlaveRole() && h.masterHost != "" && h.masterPort != "" {
		go h.reconnectMaster()
	}

}
func (h *Handler) propagateToSlaves(cmdLine [][]byte) {
	reply := protocol.NewMultiBulkReply(cmdLine)
	data := reply.ToBytes()
	h.appendBacklog(data)

	for _, slave := range h.getOnlineSlaves() {
		_, _ = slave.conn.Write(data)

	}
}
func (h *Handler) receiveMasterCommands() {
	ackTicker := time.NewTicker(time.Second)
	defer ackTicker.Stop()

	for {
		select {
		case payload, ok := <-h.masterChan:
			if !ok {
				h.handleMasterLinkDown()
				return
			}
			if payload.Err != nil {
				if payload.Err == io.EOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
					h.handleMasterLinkDown()
					return
				}
				continue
			}
			if len(payload.Data) == 0 {
				continue
			}
			reply := protocol.NewMultiBulkReply(payload.Data)
			h.addSlaveReplOffset(int64(len(reply.ToBytes())))
			_ = h.Exec(h.masterConn, payload.Data)
		case <-ackTicker.C:
			if err := h.sendAckToMaster(); err != nil {
				h.handleMasterLinkDown()
				return
			}
		case <-h.masterCtx.Done():
			return
		}
	}
}
func (h *Handler) sendAckToMaster() error {
	if h.masterConn == nil {
		return nil
	}
	cmdLine := [][]byte{
		[]byte("REPLCONF"),
		[]byte("ACK"),
		[]byte(strconv.FormatInt(h.slaveReplOffset, 10)),
	}
	replyAck := protocol.NewMultiBulkReply(cmdLine)
	_, err := h.masterConn.Write(replyAck.ToBytes())
	return err

}
func (h *Handler) afterClientClose(c redis.Connection) {
	pubsub.UnSubscribe(h.hub, c, nil)
	if c.IsSlave() {
		h.removeSlave(c)
	}
}
func (h *Handler) removeSlave(c redis.Connection) {
	h.masterStatus.mu.Lock()
	defer h.masterStatus.mu.Unlock()

	slave := h.masterStatus.slaveMap[c]
	if slave != nil {
		delete(h.masterStatus.onlineSlaves, slave)
	}
	delete(h.masterStatus.slaveMap, c)

}
func (h *Handler) getReplOffset() int64 {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	return h.masterStatus.backlog.getCurrentOffset()
}
func (h *Handler) appendBacklog(data []byte) int64 {
	h.masterStatus.mu.Lock()
	defer h.masterStatus.mu.Unlock()

	h.masterStatus.backlog.appendBytes(data)
	return h.masterStatus.backlog.getCurrentOffset()
}

func (h *Handler) getSlaveReplOffset() int64 {
	return atomic.LoadInt64(&h.slaveReplOffset)
}

func (h *Handler) addSlaveReplOffset(delta int64) int64 {
	return atomic.AddInt64(&h.slaveReplOffset, delta)
}
func (h *Handler) getReplID() string {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	return h.masterStatus.replId

}
func (h *Handler) getBacklogSnapshot() ([]byte, int64, int64) {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	return h.masterStatus.backlog.snapshot()
}
func (h *Handler) getBacklogSnapshotAfter(offset int64) ([]byte, int64, bool) {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	return h.masterStatus.backlog.snapshotAfter(offset)
}
func (h *Handler) tryPartialResync(replID string, offset int64) ([]byte, int64, bool) {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	if replID != h.masterStatus.replId {
		return nil, 0, false
	}
	return h.masterStatus.backlog.snapshotAfter(offset)

}
