package aof

import (
	"go-Redis/database"
	"go-Redis/redis/parser"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Persister struct {
	file       *os.File
	mu         sync.Mutex
	filename   string
	rewriting  bool
	rewriteBuf []byte
}

func NewPersister(filename string) (*Persister, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Persister{
		file:     file,
		filename: filename,
	}, nil

}
func (p *Persister) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.file.Close()
}
func (p *Persister) WriteCmd(cmdLine [][]byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data := marshalCmd(cmdLine)
	if _, err := p.file.Write(data); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}
	if p.rewriting {
		p.rewriteBuf = append(p.rewriteBuf, data...)
	}
	return nil
}
func (p *Persister) Load(dbSet *database.DBSet) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.file.Seek(0, 0); err != nil {
		return err
	}
	ch := parser.ParseStream(p.file)
	selectDB := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return payload.Err
		}
		if len(payload.Data) == 0 {
			continue
		}
		cmd := strings.ToUpper(string(payload.Data[0]))
		if cmd == "SELECT" {
			if len(payload.Data) != 2 {
				return io.ErrUnexpectedEOF
			}
			index, err := strconv.Atoi(string(payload.Data[1]))
			if err != nil {
				return err
			}
			if dbSet.GetDB(index) == nil {
				return io.ErrUnexpectedEOF
			}
			selectDB = index
			continue
		}
		db := dbSet.GetDB(selectDB)
		if db == nil {
			return io.ErrUnexpectedEOF
		}
		db.Exec(payload.Data)
	}
	_, err := p.file.Seek(0, 2)
	return err
}
func (p *Persister) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p == nil || p.file == nil {
		return nil
	}
	return p.file.Sync()

}
func (p *Persister) Rewrite(dbSet *database.DBSet) error {
	p.mu.Lock()
	p.rewriting = true
	p.rewriteBuf = nil
	p.mu.Unlock()
	tmpfile, err := os.CreateTemp("", "go-redis-rewrite-*.aof")
	if err != nil {
		p.mu.Lock()
		p.rewriting = false
		p.mu.Unlock()
		return err
	}
	tmpName := tmpfile.Name()
	for i := 0; i < 16; i++ {
		db := dbSet.GetDB(i)
		if db == nil {
			continue
		}
		if len(db.Data) == 0 {
			continue
		}
		selectedCmd := [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(i)),
		}
		if _, err := tmpfile.Write(marshalCmd(selectedCmd)); err != nil {
			tmpfile.Close()
			p.mu.Lock()
			p.rewriting = false
			p.mu.Unlock()
			return err
		}
		for key, entity := range db.Data {
			if entity == nil {
				continue
			}
			if entity.Type != "string" {
				continue
			}
			value, ok := entity.Data.([]byte)
			if !ok {
				continue
			}
			cmdLine := [][]byte{
				[]byte("SET"),
				[]byte(key),
				value,
			}
			if _, err := tmpfile.Write(marshalCmd(cmdLine)); err != nil {
				tmpfile.Close()
				p.mu.Lock()
				p.rewriting = false
				p.mu.Unlock()
				return err
			}

		}

	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.rewriteBuf) > 0 {
		if _, err := tmpfile.Write(p.rewriteBuf); err != nil {
			p.rewriting = false
			p.rewriteBuf = nil
			tmpfile.Close()
			return err
		}
	}

	if err := tmpfile.Sync(); err != nil {
		tmpfile.Close()
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}
	if err := tmpfile.Close(); err != nil {
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}

	if err := p.file.Close(); err != nil {
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}
	_ = os.Remove(p.filename)

	if err := os.Rename(tmpName, p.filename); err != nil {
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}

	file, err := os.OpenFile(p.filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}

	p.file = file
	p.rewriting = false
	p.rewriteBuf = nil

	return nil

}

func marshalCmd(cmdLine [][]byte) []byte {
	var result []byte
	result = append(result, []byte("*"+strconv.Itoa(len(cmdLine))+"\r\n")...)
	for _, arg := range cmdLine {
		result = append(result, []byte("$"+strconv.Itoa(len(arg))+"\r\n")...)
		result = append(result, arg...)
		result = append(result, []byte("\r\n")...)
	}
	return result

}
