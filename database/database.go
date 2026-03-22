package database

type DB struct {
	Data map[string]*DataEntity
}
type DataEntity struct {
	Type string
	Data any
}

func NewDB() *DB {
	return &DB{
		Data: make(map[string]*DataEntity),
	}
}
