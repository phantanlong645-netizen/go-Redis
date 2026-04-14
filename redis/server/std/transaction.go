package std

import (
	"go-Redis/database"
	"go-Redis/interface/redis"
	"go-Redis/redis/protocol"
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
func (h *Handler) enqueueCmdInMulti(c redis.Connection, cmdLine [][]byte) protocol.Reply {
	cmd := strings.ToUpper(string(cmdLine[0]))
	switch cmd {
	case "SELECT", "BGREWRITEAOF", "SAVE", "BGSAVE",
		"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE",
		"SLAVEOF", "REPLCONF", "PSYNC", "WAIT":
		errReply := protocol.NewErrReply("ERR command '" + cmd + "' cannot be used in MULTI")
		c.AddTxError(errReply)
		return errReply
	}
	cmdObj, ok := database.Router[cmd]
	if !ok {
		errReply := protocol.NewErrReply("ERR unknown command '" + strings.ToLower(cmd) + "'")
		c.AddTxError(errReply)
		return errReply
	}
	args := cmdLine[1:]
	if cmdObj.Arity >= 0 {
		if cmdObj.Arity != len(args) {
			errReply := protocol.NewErrReply("ERR wrong number of arguments for " + cmd)
			c.AddTxError(errReply)
			return errReply
		}
	} else {
		if len(args) < -cmdObj.Arity {
			errReply := protocol.NewErrReply("ERR wrong number of arguments for " + cmd)
			c.AddTxError(errReply)
			return errReply
		}
	}
	c.EnqueueCmd(cmdLine)
	return protocol.NewStatusReply("QUEUED")

}
