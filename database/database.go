package database

import "time"

const dbNum = 16

type DBSet struct {
	dbs []*DB
}
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
func NewDBSet() *DBSet {
	dbs := make([]*DB, dbNum)
	for i := 0; i < dbNum; i++ {
		dbs[i] = NewDB()
	}
	return &DBSet{
		dbs: dbs,
	}
}
func (d *DBSet) GetDB(index int) *DB {
	if index < 0 || index >= len(d.dbs) {
		return nil
	}
	return d.dbs[index]
}
