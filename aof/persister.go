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
	currentDB  int
	rewriting  bool
	rewriteBuf []aofPayload
}
type aofPayload struct {
	dbIndex int
	cmdLine [][]byte
}
type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
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
func (p *Persister) SaveCmdLine(dbIndex int, cmdLine [][]byte) error {

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(cmdLine) == 0 {
		return nil
	}
	cmdName := strings.ToUpper(string(cmdLine[0]))
	if cmdName == "SELECT" {
		data := marshalCmd(cmdLine)
		if _, err := p.file.Write(data); err != nil {
			return err
		}
		if err := p.file.Sync(); err != nil {
			return err
		}
		p.currentDB = dbIndex
		if p.rewriting {
			p.rewriteBuf = append(p.rewriteBuf, aofPayload{
				dbIndex: dbIndex,
				cmdLine: cloneCmdLine(cmdLine),
			})
		}
		return nil
	}
	if dbIndex != p.currentDB {
		selectCmd := [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(dbIndex)),
		}
		if _, err := p.file.Write(marshalCmd(selectCmd)); err != nil {
			return err
		}
		p.currentDB = dbIndex
	}
	data := marshalCmd(cmdLine)
	if _, err := p.file.Write(data); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}
	if p.rewriting {
		p.rewriteBuf = append(p.rewriteBuf, aofPayload{
			dbIndex: dbIndex,
			cmdLine: cloneCmdLine(cmdLine),
		})
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
	ctx, err := p.StartRewrite()
	if err != nil {
		return err
	}
	if err := p.DoRewrite(ctx, dbSet); err != nil {
		return err
	}
	return p.FinishRewrite(ctx)
}
func (p *Persister) StartRewrite() (*RewriteCtx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.rewriting = true
	p.rewriteBuf = nil
	if err := p.file.Sync(); err != nil {
		p.rewriting = false
		return nil, err
	}
	fileInfo, err := os.Stat(p.filename)
	if err != nil {
		p.rewriting = false
		return nil, err
	}
	tmpFile, err := os.CreateTemp("", "go-redis-rewrite-*.aof")
	if err != nil {
		p.rewriting = false
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  tmpFile,
		fileSize: fileInfo.Size(),
	}, nil

}
func (p *Persister) DoRewrite(ctx *RewriteCtx, dbSet *database.DBSet) error {
	tmpFile := ctx.tmpFile
	for i := 0; i < 16; i++ {
		db := dbSet.GetDB(i)
		if db == nil {
			continue
		}
		snapshot := snapshotDB(db)
		if len(snapshot) == 0 {
			continue
		}
		selectCmd := [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(i)),
		}
		if _, err := tmpFile.Write(marshalCmd(selectCmd)); err != nil {
			return err
		}
		for key, entity := range snapshot {
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
			if _, err := tmpFile.Write(marshalCmd(cmdLine)); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *Persister) FinishRewrite(ctx *RewriteCtx) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmpFile := ctx.tmpFile
	tmpName := tmpFile.Name()
	currentDB := -1
	if len(p.rewriteBuf) > 0 {
		for _, payload := range p.rewriteBuf {
			if payload.dbIndex != currentDB {
				selectCMD := [][]byte{
					[]byte("SELECT"),
					[]byte(strconv.Itoa(payload.dbIndex)),
				}
				if _, err := tmpFile.Write(marshalCmd(selectCMD)); err != nil {
					_ = tmpFile.Close()
					p.rewriting = false
					p.rewriteBuf = nil
					return err
				}
				currentDB = payload.dbIndex
			}
			if _, err := tmpFile.Write(marshalCmd(payload.cmdLine)); err != nil {
				_ = tmpFile.Close()
				p.rewriting = false
				p.rewriteBuf = nil
				return err
			}
		}
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		p.rewriteBuf = nil
		return err
	}

	if err := tmpFile.Close(); err != nil {
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
func snapshotDB(db *database.DB) map[string]*database.DataEntity {
	if db == nil {
		return nil
	}

	result := make(map[string]*database.DataEntity, len(db.Data))
	db.Mu.RLock()
	defer db.Mu.RUnlock()
	for key, entity := range db.Data {
		if entity == nil {
			continue
		}
		if entity.Type == "string" {
			value, ok := entity.Data.([]byte)
			if !ok {
				continue
			}
			copidValue := append([]byte(nil), value...)
			result[key] = &database.DataEntity{
				Type: "string",
				Data: copidValue,
			}
			continue
		}
		result[key] = entity
	}
	return result
}
func cloneCmdLine(cmdLine [][]byte) [][]byte {
	res := make([][]byte, len(cmdLine))
	for i, arg := range cmdLine {
		res[i] = append(res[i], arg...)
	}
	return res
}
