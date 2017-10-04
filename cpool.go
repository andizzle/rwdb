package rwdb

import (
	"database/sql"
	"errors"
	"sync"
)

var cplock sync.RWMutex

// CPool holds the  master and slave connection pools
type CPool struct {
	pool []*sql.DB
	next int
}

func (c *CPool) nextInPool() int {
	c.next = (c.next + 1) % len(c.pool)

	return c.next
}

// AddReader append a db connection to the pool
func (c *CPool) AddReader(db *sql.DB) {
	cplock.RLock()

	if len(c.pool) > 1 {
		for i, po := range c.pool[1:] {
			if po == nil {
				c.pool[i+1] = db
				return
			}
		}
	}

	c.pool = append(c.pool, db)
	cplock.RUnlock()
}

// AddWriter prepend a db connection to the pool
// Previous writer automatically become a reader
func (c *CPool) AddWriter(db *sql.DB) {
	c.pool = append([]*sql.DB{db}, c.pool...)
}

// Reader gets the reader connection next in line
func (c *CPool) Reader() (*sql.DB, error) {
	pos := c.nextInPool()

	cplock.RLock()

	conn := c.pool[pos]

	var count int

	for conn == nil {
		if count = c.nextInPool(); count == pos {
			return nil, errors.New("no reader db available")
		}

		conn = c.pool[count]
	}

	cplock.RUnlock()

	return conn, nil
}

// Writer gets the writer connection
func (c *CPool) Writer() (*sql.DB, error) {
	db := c.pool[0]

	if db == nil {
		return db, errors.New("no writer db available")
	}

	return db, nil
}
