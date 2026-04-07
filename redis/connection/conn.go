package connection

import (
	"net"
	"sync"
)

const (
	flagSlave = uint64(1 << iota)
	flagMaster
)

type Connection struct {
	conn       net.Conn
	selectedDB int
	subs       map[string]struct{}
	mu         sync.Mutex
	flags      uint64
	offset     int64
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
func (c *Connection) GetOffset() int64 {
	return c.offset
}
func (c *Connection) SetOffset(offset int64) {
	c.offset = offset
}
func (c *Connection) AddOffset(delta int64) {
	c.offset += delta
}
