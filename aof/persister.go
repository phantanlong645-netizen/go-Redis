package aof

import (
	"context"
	"go-Redis/database"
	"go-Redis/redis/parser"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	FsyncNo       = "no"
)

type Persister struct {
	fsyncPolicy string
	file        *os.File
	mu          sync.Mutex
	filename    string
	currentDB   int
	rewriting   bool
	ctx         context.Context
	cancel      context.CancelFunc
	aofChan     chan *payload
	aofFinished chan struct{}
}

type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
}
type snapshotEntry struct {
	entity   *database.DataEntity
	expireAt *time.Time
}
type payload struct {
	cmdLine [][]byte
	dbIndex int
}

func NewPersister(filename string, fsyncPolicy string) (*Persister, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	p := &Persister{
		file:        file,
		filename:    filename,
		fsyncPolicy: strings.ToLower(fsyncPolicy),
		ctx:         ctx,
		cancel:      cancel,
		aofChan:     make(chan *payload, 1<<20),
		aofFinished: make(chan struct{}),
	}
	go p.listenCmd()
	if p.fsyncPolicy == FsyncEverySec {
		p.FsyncEverySecond()
	}
	return p, nil

}
func (p *Persister) FsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = p.Sync()
			case <-p.ctx.Done():
				return
			}
		}
	}()

}
func (p *Persister) listenCmd() {
	for pl := range p.aofChan {
		_ = p.writeAof(pl)
	}
	p.aofFinished <- struct{}{}
}
func (p *Persister) writeAof(pl *payload) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(pl.cmdLine) == 0 {
		return nil
	}
	cmdName := strings.ToUpper(string(pl.cmdLine[0]))
	if cmdName == "SELECT" {
		data := marshalCmd(pl.cmdLine)
		if _, err := p.file.Write(data); err != nil {
			return err
		}
		p.currentDB = pl.dbIndex
		if p.fsyncPolicy == FsyncAlways {
			return p.file.Sync()
		}
		return nil
	}
	if p.currentDB != pl.dbIndex {
		selectCmd := [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(pl.dbIndex)),
		}
		if _, err := p.file.Write(marshalCmd(selectCmd)); err != nil {
			return err
		}
		p.currentDB = pl.dbIndex
	}
	if _, err := p.file.Write(marshalCmd(pl.cmdLine)); err != nil {
		return err
	}
	if p.fsyncPolicy == FsyncAlways {
		if err := p.file.Sync(); err != nil {
			return err
		}
	}
	return nil

}

func (p *Persister) Close() error {
	if p == nil {
		return nil
	}
	if p.aofChan != nil {
		close(p.aofChan)
		<-p.aofFinished
	}
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			if p.cancel != nil {
				p.cancel()
			}
			return err
		}
	}
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}
func (p *Persister) SaveCmdLine(dbIndex int, cmdLine [][]byte) error {
	if len(cmdLine) == 0 {
		return nil
	}
	if p.fsyncPolicy == FsyncAlways {
		return p.writeAof(&payload{
			dbIndex: dbIndex,
			cmdLine: cloneCmdLine(cmdLine),
		})
	}
	p.aofChan <- &payload{
		dbIndex: dbIndex,
		cmdLine: cloneCmdLine(cmdLine),
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
			if entity.entity == nil {
				continue
			}

			if entity.entity.Type != "string" {
				continue
			}
			value, ok := entity.entity.Data.([]byte)
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
			if entity.expireAt != nil {
				ex := [][]byte{
					[]byte("PEXPIREAT"),
					[]byte(key),
					[]byte(strconv.FormatInt(entity.expireAt.UnixMilli(), 10)),
				}
				if _, err := tmpFile.Write(marshalCmd(ex)); err != nil {
					return err
				}

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
	src, err := os.Open(p.filename)
	if err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		return err
	}

	if _, err := src.Seek(ctx.fileSize, 0); err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		return err
	}
	if _, err := io.Copy(tmpFile, src); err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		return err
	}
	if err := src.Close(); err != nil {
		_ = tmpFile.Close()
		p.rewriting = false
		return err
	}

	if err := tmpFile.Close(); err != nil {
		p.rewriting = false
		return err
	}
	if err := p.file.Close(); err != nil {
		p.rewriting = false
		return err
	}

	_ = os.Remove(p.filename)

	if err := os.Rename(tmpName, p.filename); err != nil {
		p.rewriting = false
		return err
	}
	file, err := os.OpenFile(p.filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		p.rewriting = false
		return err
	}
	p.file = file
	p.rewriting = false
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
func snapshotDB(db *database.DB) map[string]snapshotEntry {
	if db == nil {
		return nil
	}

	result := make(map[string]snapshotEntry, len(db.Data))
	db.Mu.RLock()
	defer db.Mu.RUnlock()
	now := time.Now()
	for key, entity := range db.Data {
		if entity == nil {
			continue
		}
		var expireAt *time.Time
		if deadline, ok := db.TTL[key]; ok {
			if !deadline.After(now) {
				continue
			}
			expireCopy := deadline
			expireAt = &expireCopy
		}
		if entity.Type == "string" {
			value, ok := entity.Data.([]byte)
			if !ok {
				continue
			}
			copidValue := append([]byte(nil), value...)
			result[key] = snapshotEntry{
				entity: &database.DataEntity{
					Type: "string",
					Data: copidValue,
				},
				expireAt: expireAt,
			}
			continue
		}
		result[key] = snapshotEntry{
			entity:   entity,
			expireAt: expireAt,
		}
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
