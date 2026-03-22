package database

type DB struct {
	Data map[string][]byte
}

func NewDB() *DB {
	return &DB{
		Data: make(map[string][]byte),
	}
}
