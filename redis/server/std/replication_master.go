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
}
