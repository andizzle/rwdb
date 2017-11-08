package rwdb

import (
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

// CPool holds the  master and slave connection pools
type CPool struct {
	pool []*sql.DB
	next uint64
	lock sync.RWMutex
}

func (c *CPool) nextInPool() int {
	if c.poolSize() == 0 {
		return -1
	}

	c.next = atomic.AddUint64(&c.next, 1) % uint64(len(c.pool))
	return int(c.next)
}

func (c *CPool) poolSize() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.pool)
}

// AddReader append a db connection to the pool
func (c *CPool) AddReader(db *sql.DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.pool) > 1 {
		for i, po := range c.pool[1:] {
			if po == nil {
				c.pool[i+1] = db
				return
			}
		}
	}

	c.pool = append(c.pool, db)
}

// AddWriter prepend a db connection to the pool
// Previous writer automatically become a reader
func (c *CPool) AddWriter(db *sql.DB) {
	c.lock.Lock()
	c.pool = append([]*sql.DB{db}, c.pool...)
	c.lock.Unlock()
}

// Reader gets the reader connection next in line
func (c *CPool) Reader() (*sql.DB, error) {
	pos := c.nextInPool()

	if pos < 0 {
		return nil, errors.New("no reader db available")
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	conn := c.pool[pos]

	var count int

	for conn == nil {
		if count = c.nextInPool(); count == pos {
			return nil, errors.New("no reader db available")
		}

		conn = c.pool[count]
	}

	return conn, nil
}

// Writer gets the writer connection
func (c *CPool) Writer() (*sql.DB, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if len(c.pool) == 0 {
		return nil, errors.New("no writer db available")
	}

	db := c.pool[0]

	if db == nil {
		return nil, errors.New("no writer db available")
	}

	return db, nil
}
