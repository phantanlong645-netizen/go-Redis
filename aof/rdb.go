package aof

import (
	"go-Redis/database"
	"os"

	"github.com/hdt3213/rdb/encoder"
)

func (p *Persister) GenerateRDB(rdbFilename string) error {
	ctx, err := p.startGenerateRDB()
	if err != nil {
		return err
	}
	err = p.generateRDB(ctx, p.dbSet)
	if err != nil {
		_ = ctx.tmpFile.Close()
		_ = os.Remove(ctx.tmpFile.Name())
		return err
	}
	if err = ctx.tmpFile.Close(); err != nil {
		_ = os.Remove(ctx.tmpFile.Name())
		return err
	}
	_ = os.Remove(rdbFilename)
	return os.Rename(ctx.tmpFile.Name(), rdbFilename)

}

func (p *Persister) startGenerateRDB() (*RewriteCtx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.file.Sync(); err != nil {
		return nil, err
	}
	tmpFile, err := os.CreateTemp("", "go-redis-*.rdb")
	if err != nil {
		return nil, err
	}
	return &RewriteCtx{
		tmpFile: tmpFile,
	}, nil
}

func (p *Persister) generateRDB(ctx *RewriteCtx, dbSet *database.DBSet) error {
	enc := encoder.NewEncoder(ctx.tmpFile)
	if err := enc.WriteHeader(); err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "7.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		if err := enc.WriteAux(k, v); err != nil {
			return err
		}
	}
	for i := 0; i < 16; i++ {
		db := dbSet.GetDB(i)
		if db == nil {
			continue
		}
		snapshot := snapshotDB(db)
		if len(snapshot) == 0 {
			continue
		}
		var ttlCount uint64
		var keyCount uint64
		for _, entry := range snapshot {
			if entry.entity == nil {
				continue
			}
			if entry.entity.Type != "string" {
				continue
			}
			if _, ok := entry.entity.Data.([]byte); !ok {
				continue
			}

			keyCount++
			if entry.expireAt != nil {
				ttlCount++
			}
		}
		if keyCount == 0 {
			continue
		}
		if err := enc.WriteDBHeader(uint(i), uint64(keyCount), ttlCount); err != nil {
			return err
		}
		for key, entry := range snapshot {
			if entry.entity == nil {
				continue
			}
			if entry.entity.Type != "string" {
				continue
			}
			value, ok := entry.entity.Data.([]byte)
			if !ok {
				continue
			}
			if entry.expireAt != nil {
				if err := enc.WriteStringObject(
					key,
					value,
					encoder.WithTTL(uint64(entry.expireAt.UnixMilli())),
				); err != nil {
					return err
				}
			} else {
				if err := enc.WriteStringObject(key, value); err != nil {
					return err
				}
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		return err
	}
	return nil
}
