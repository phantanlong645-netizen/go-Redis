package database

import "time"

type DB struct {
	Data map[string]*DataEntity
	TTL  map[string]time.Time
}
type DataEntity struct {
	Type string
	Data any
}

func NewDB() *DB {
	return &DB{
		Data: make(map[string]*DataEntity),
		TTL:  make(map[string]time.Time),
	}
}
