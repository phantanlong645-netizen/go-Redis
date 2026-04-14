package std

import (
	"bufio"
	"context"
	"fmt"
	"go-Redis/interface/redis"
	"go-Redis/redis/connection"
	"go-Redis/redis/protocol"
	"net"
	"strconv"
	"strings"
)

func (h *Handler) execMulti(c redis.Connection) protocol.Reply {
	if c.InMultiState() {
		return protocol.NewErrReply("ERR MULTI calls can not be nested")
	}
	c.SetMultiState(true)
	c.ClearQueuedCmds()
	return protocol.NewStatusReply("OK")
}
func (h *Handler) execDiscard(c redis.Connection) protocol.Reply {
	if !c.InMultiState() {
		return protocol.NewErrReply("ERR DISCARD without MULTI")
	}
	c.ClearQueuedCmds()
	c.SetMultiState(false)
	return protocol.NewStatusReply("OK")

}
func (h *Handler) execExec(c redis.Connection) protocol.Reply {
	if !c.InMultiState() {
		return protocol.NewErrReply("ERR Exec without MULTI")
	}
	if len(c.GetTxErrors()) > 0 {
		c.SetMultiState(false)
		return protocol.NewErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	queued := c.GetQueuedCmdLine()
	c.ClearQueuedCmds()
	c.SetMultiState(false)
	replies := make([]protocol.Reply, 0, len(queued))
	for _, cmdLine := range queued {
		reply := h.Exec(c, cmdLine)
		replies = append(replies, reply)
	}
	return protocol.NewMultiRawReply(replies)

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
