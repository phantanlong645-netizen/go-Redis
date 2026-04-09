package std

import (
	"go-Redis/interface/redis"
	"sync"
	"time"
)

const (
	slaveStateHandshake = uint8(iota)
	slaveStateOnline
)

type slaveClient struct {
	conn        redis.Connection
	state       uint8
	offset      int64
	lastAckTime time.Time
}
type masterStatus struct {
	mu           sync.RWMutex
	slaveMap     map[redis.Connection]*slaveClient
	onlineSlaves map[*slaveClient]struct{}
	backlog      *replBacklog
	replId       string
}
type replBacklog struct {
	buf           []byte
	beginOffset   int64
	currentOffset int64
	maxSize       int64
}

func newReplBacklog(maxSize int64) *replBacklog {
	return &replBacklog{
		buf:           make([]byte, 0, maxSize),
		beginOffset:   0,
		currentOffset: 0,
		maxSize:       maxSize,
	}
}
func initMasterStatus(replId string, backlogSize int64) *masterStatus {
	return &masterStatus{
		replId:       replId,
		slaveMap:     make(map[redis.Connection]*slaveClient),
		onlineSlaves: make(map[*slaveClient]struct{}),
		backlog:      newReplBacklog(backlogSize),
	}
}
func (h *Handler) getOrCreateSlave(c redis.Connection) *slaveClient {
	h.masterStatus.mu.Lock()
	defer h.masterStatus.mu.Unlock()

	if slave, ok := h.masterStatus.slaveMap[c]; ok {
		return slave
	}
	slave := &slaveClient{
		conn:        c,
		state:       slaveStateHandshake,
		offset:      0,
		lastAckTime: time.Now(),
	}
	h.masterStatus.slaveMap[c] = slave
	return slave
}
func (h *Handler) setSlaveOnline(slave *slaveClient, offset int64) {
	h.masterStatus.mu.Lock()
	defer h.masterStatus.mu.Unlock()
	slave.state = slaveStateOnline
	slave.offset = offset
	slave.lastAckTime = time.Now()
	h.masterStatus.onlineSlaves[slave] = struct{}{}

}
func (h *Handler) getOnlineSlaves() []*slaveClient {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()
	res := make([]*slaveClient, 0, len(h.masterStatus.onlineSlaves))
	for slave := range h.masterStatus.onlineSlaves {
		res = append(res, slave)
	}
	return res
}
func (b *replBacklog) appendBytes(p []byte) {
	if len(p) == 0 {
		return
	}
	b.buf = append(b.buf, p...)
	b.currentOffset += int64(len(p))
	if int64(len(b.buf)) <= b.maxSize {
		return
	}
	extra := int64(len(b.buf)) - b.maxSize
	b.buf = append([]byte(nil), b.buf[extra:]...)
	b.beginOffset += extra

}
func (b *replBacklog) getCurrentOffset() int64 {
	return b.currentOffset
}
func (h *Handler) countOnlineSlavesAtOffset(offset int64) int {
	h.masterStatus.mu.RLock()
	defer h.masterStatus.mu.RUnlock()

	count := 0
	for slave := range h.masterStatus.onlineSlaves {
		if slave.offset >= offset {
			count++
		}
	}
	return count
}
func (b *replBacklog) isValidOffset(offset int64) bool {
	return offset >= b.beginOffset && offset <= b.currentOffset
}
func (b *replBacklog) snapshot() ([]byte, int64, int64) {
	data := append([]byte(nil), b.buf...)
	return data, b.beginOffset, b.currentOffset
}
func (b *replBacklog) snapshotAfter(offset int64) ([]byte, int64, bool) {
	if !b.isValidOffset(offset) {
		return nil, 0, false
	}
	start := offset - b.beginOffset
	data := append([]byte(nil), b.buf[start:]...)
	return data, b.currentOffset, true

}
