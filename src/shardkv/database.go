package shardkv

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

func (db *DataBase) GetShard(shard int) map[string]string {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	result := make(map[string]string)
	for k, v := range db.Table {
		if key2shard(k) == shard {
			result[k] = v
		}
	}
	return result
}
func (db *DataBase) SetShard(data map[string]string) {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	for k, v := range data {
		db.Table[k] = v
	}
}
func (db *DataBase) DelShard(shard int) {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	var result []string
	for k, _ := range db.Table {
		if key2shard(k) == shard {
			result = append(result, k)
		}
	}
	for _, k := range result {
		delete(db.Table, k)
	}
}
