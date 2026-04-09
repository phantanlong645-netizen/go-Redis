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
	replOffset   int64
}

func initMasterStatus() *masterStatus {
	return &masterStatus{
		slaveMap:     make(map[redis.Connection]*slaveClient),
		onlineSlaves: make(map[*slaveClient]struct{}),
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
