package kvraft

import "sync"

type DataBase struct {
	Mu    sync.Mutex
	Table map[string]string
}

func (db *DataBase) Get(key string) (string, Err) {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	if value, ok := db.Table[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (db *DataBase) Put(key string, value string) Err {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	db.Table[key] = value
	return OK
}

func (db *DataBase) Append(key string, value string) Err {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	if _, ok := db.Table[key]; ok {
		db.Table[key] += value
	} else {
		db.Table[key] = value
	}
	return OK
}
