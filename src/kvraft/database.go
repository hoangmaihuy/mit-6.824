package kvraft

import "sync"

type Database struct {
	mu sync.Mutex
	data map[string]string
}

func (db *Database) Get(key string) (string, Err) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if elem, ok := db.data[key]; ok {
		return elem, OK
	} else {
		return "", ErrNoKey
	}
}

func (db *Database) Put(key string, value string) (string, Err) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
	return value, OK
}

func (db *Database) Append(key string, value string) (string, Err) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if elem, ok := db.data[key]; ok {
		db.data[key] = elem + value
	} else {
		db.data[key] = value
	}
	return db.data[key], OK
}