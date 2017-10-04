package rwdb

import (
	"database/sql"
	"testing"
)

func TestNextInPool(t *testing.T) {
	var c = CPool{pool: make([]*sql.DB, 5)}

	if got := c.nextInPool(); got != 1 {
		t.Errorf("expect next position to be 1, got %d", got)
	}

	for n := 1; n < 5; n++ {
		c.nextInPool()
	}

	if c.next != 0 {
		t.Errorf("expect next position to be 0, got %d", c.next)
	}
}

func TestAddReader(t *testing.T) {
	var c = CPool{pool: make([]*sql.DB, 5)}

	c.AddReader(&sql.DB{})

	if len(c.pool) != 5 {
		t.Errorf("expect number of connections in pool to be 5, got %d", len(c.pool))
		t.Error(c.pool)
	}

	c = CPool{pool: make([]*sql.DB, 1)}

	c.AddReader(&sql.DB{})

	if len(c.pool) != 2 {
		t.Errorf("expect number of connections in pool to be 2, got %d", len(c.pool))
	}
}

func TestAddWriter(t *testing.T) {
	var c = CPool{pool: []*sql.DB{}}

	db1 := &sql.DB{}
	c.AddWriter(db1)

	if c.pool[0] != db1 {
		t.Errorf("expect writer to be %v, got %s instead", db1, c.pool[0])
	}

	db2 := &sql.DB{}
	c.AddWriter(db2)

	if c.pool[0] != db2 {
		t.Errorf("expect writer to be %v, got %s instead", db2, c.pool[0])
	}

	if c.pool[1] != db1 {
		t.Errorf("expect reader to be %v, got %s instead", db1, c.pool[1])
	}
}

func TestGetReader(t *testing.T) {
	var c = CPool{pool: make([]*sql.DB, 5)}

	db, err := c.Reader()

	if err == nil {
		t.Errorf("expecting error, got %v instead", db)
	}
}
