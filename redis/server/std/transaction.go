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
	if h.isWatchingChanged(c) {
		c.SetMultiState(false)
		return protocol.NewNullMultiBulkReply()
	}
	queued := c.GetQueuedCmdLine()
	db := h.dbSet.GetDB(c.GetDBIndex())
	if db == nil {
		c.SetMultiState(false)
		return protocol.NewErrReply("ERR no such database")
	}
	undoLogs := make([][][][]byte, 0, len(queued))

	c.SetMultiState(false)
	replies := make([]protocol.Reply, 0, len(queued))
	for i, cmdLine := range queued {
		undolog := db.GetUndoLogs(cmdLine)
		reply := h.Exec(c, cmdLine)
		replies = append(replies, reply)
		if isErrorReply(reply) {
			h.rollback(db, undoLogs[:i])
			break
		}
		undoLogs = append(undoLogs, undolog)
	}
	return protocol.NewMultiRawReply(replies)

}
func (h *Handler) rollback(db *database.DB, undoLogs [][][][]byte) {
	for i := len(undoLogs) - 1; i >= 0; i-- {
		logs := undoLogs[i]
		for j := len(logs) - 1; j >= 0; j-- {
			db.Exec(logs[j])
		}
	}
}

func (h *Handler) execWatch(c redis.Connection, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) < 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for WATCH")
	}
	if c.InMultiState() {
		return protocol.NewErrReply("ERR WATCH inside MULTI is not allowed")
	}
	db := h.dbSet.GetDB(c.GetDBIndex())
	if db == nil {
		return protocol.NewErrReply("ERR no such database")
	}
	dbIndex := c.GetDBIndex()
	watching := c.GetWatching()
	dbwatching, ok := watching[dbIndex]
	if !ok {
		dbwatching = make(map[string]uint32)
		watching[dbIndex] = dbwatching
	}
	for _, arg := range cmdLine[1:] {
		key := string(arg)
		watching[dbIndex][key] = db.GetVersion(key)
	}
	return protocol.NewStatusReply("OK")

}
func (h *Handler) execUnWatch(c redis.Connection, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for UNWATCH")
	}
	c.ClearWatching()
	return protocol.NewStatusReply("OK")
}
func (h *Handler) enqueueCmdInMulti(c redis.Connection, cmdLine [][]byte) protocol.Reply {
	cmd := strings.ToUpper(string(cmdLine[0]))
	switch cmd {
	case "SELECT", "BGREWRITEAOF", "SAVE", "BGSAVE",
		"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE",
		"SLAVEOF", "REPLCONF", "PSYNC", "WAIT",
		"WATCH", "UNWATCH":
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
func (h *Handler) isWatchingChanged(c redis.Connection) bool {
	watching := c.GetWatching()
	for dbIndex, keyMap := range watching {
		db := h.dbSet.GetDB(dbIndex)
		if db == nil {
			return true
		}
		for key, ver := range keyMap {
			if db.GetVersion(key) != ver {
				return true
			}
		}
	}
	return false
}
