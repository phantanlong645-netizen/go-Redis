package connection

import (
	"go-Redis/redis/protocol"
	"net"
	"sync"
)

const (
	flagSlave = uint64(1 << iota)
	flagMaster
	flagMulti
)

type Connection struct {
	conn       net.Conn
	selectedDB int
	subs       map[string]struct{}
	queue      [][][]byte
	mu         sync.Mutex
	flags      uint64
	txErrors   []protocol.Reply
	watching   map[int]map[string]uint32
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
		subs: make(map[string]struct{}),
	}
}
func (c *Connection) Write(b []byte) (int, error) {
	return c.conn.Write(b)
}
func (c *Connection) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[channel] = struct{}{}
}
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, channel)
}
func (c *Connection) SubsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subs)
}
func (c *Connection) GetChannels() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	channels := make([]string, 0, len(c.subs))
	for channel := range c.subs {
		channels = append(channels, channel)
	}
	return channels
}
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbIndex int) {
	c.selectedDB = dbIndex
}
func (c *Connection) SetSlave() {
	c.flags |= flagSlave

}
func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}
func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}
func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
func (c *Connection) SetMultiState(state bool) {
	if state {
		c.flags |= flagMulti
		return
	}
	c.flags &^= flagMulti
	c.queue = nil
	c.txErrors = nil
	c.watching = nil
}
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cloned := make([][]byte, len(cmdLine))
	for i, arg := range cmdLine {
		cloned[i] = append([]byte(nil), arg...)
	}
	c.queue = append(c.queue, cloned)
}
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make([][][]byte, len(c.queue))
	for i, cmdLine := range c.queue {
		cloned := make([][]byte, len(cmdLine))
		for j, arg := range cmdLine {
			cloned[j] = append([]byte(nil), arg...)
		}
		res[i] = cloned
	}
	return res

}
func (c *Connection) ClearQueuedCmds() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue = c.queue[:0]
}
func (c *Connection) AddTxError(reply protocol.Reply) {
	if reply == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txErrors = append(c.txErrors, reply)

}
func (c *Connection) GetTxErrors() []protocol.Reply {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make([]protocol.Reply, len(c.txErrors))
	copy(res, c.txErrors)
	return res
}
func (c *Connection) GetWatching() map[int]map[string]uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watching == nil {
		c.watching = make(map[int]map[string]uint32)
	}
	return c.watching
}
func (c *Connection) ClearWatching() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watching = nil
}
