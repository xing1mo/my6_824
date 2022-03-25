package kvraft

import "sync"

type DataBase struct {
	mu    sync.Mutex
	table map[string]string
}

func (db *DataBase) Get(key string) (string, Err) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if value, ok := db.table[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (db *DataBase) Put(key string, value string) Err {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.table[key] = value
	return OK
}

func (db *DataBase) Append(key string, value string) Err {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.table[key]; ok {
		db.table[key] += value
	} else {
		db.table[key] = value
	}
	return OK
}
