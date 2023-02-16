package main

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type DBStore struct {
	db *leveldb.DB
}

func NewDBStore(dataDir string) (*DBStore, error) {
	dbStore := &DBStore{}

	db, err := leveldb.OpenFile(dataDir+"/db", nil)
	if err != nil {
		return nil, err
	}

	dbStore.db = db
	return dbStore, nil
}

func (c *DBStore) Get(key string) (string, error) {
	value, err := c.db.Get([]byte(key), nil)
	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (c *DBStore) Set(key string, value string) error {
	return c.db.Put([]byte(key), []byte(value), nil)
}

func (c *DBStore) Close() {
	if c.db != nil {
		c.db.Close()
	}
}
