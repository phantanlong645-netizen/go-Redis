package aof

import "go-Redis/database"

func (p *Persister) GenerateRDB(rdbFilename string, dbSet *database.DBSet) error {
	ctx, err := p.startGenerateRDB()
	if err != nil {
		return err
	}
	err = p.generateRDB(ctx, dbSet)
	if err != nil {
		return err
	}
	return nil

}

func (p *Persister) startGenerateRDB() (*RewriteCtx, error) {
	return nil, nil
}

func (p *Persister) generateRDB(ctx *RewriteCtx, dbSet *database.DBSet) error {
	return nil
}
