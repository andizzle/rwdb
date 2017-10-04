package rwdb

import (
	"database/sql"
	"sync"
)

var cplock sync.RWMutex

// CPool holds the  master and slave connection pools
type CPool struct {
	pool []*sql.DB
	next int
}

func (c *CPool) nextInPool() int {
	c.next++
	pos := c.next % len(c.pool)

	return pos
}

// AddReader append a db connection to the pool
func (c *CPool) AddReader(db *sql.DB) {
	cplock.RLock()
	c.pool = append(c.pool, db)
	cplock.RUnlock()
}

// AddWriter prepend a db connection to the pool
// Previous writer automatically become a reader
func (c *CPool) AddWriter(db *sql.DB) {
	c.pool = append([]*sql.DB{db}, c.pool...)
}

// Reader gets the reader connection next in line
func (c *CPool) Reader() *sql.DB {
	pos := c.nextInPool()

	// TODO: create timeout context
	cplock.RLock()

	conn := c.pool[pos]

	for conn == nil {
		conn = c.pool[c.nextInPool()]
	}

	cplock.RUnlock()

	return conn
}

// Writer gets the writer connection
func (c *CPool) Writer() *sql.DB {
	return c.pool[0]
}
