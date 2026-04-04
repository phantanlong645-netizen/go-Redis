package connection

import (
	"net"
	"sync"
)

type Connection struct {
	conn       net.Conn
	selectedDB int
	subs       map[string]bool
	mu         sync.Mutex
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
		subs: make(map[string]bool),
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
	c.subs[channel] = true
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
