package rwdb

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

type Dummy struct {
	driver.Driver
}

func init() {
	sql.Register("dummy", Dummy{})
}

func TestOpen(t *testing.T) {
	_, err := Open("test")

	if err == nil {
		t.Errorf("expect no data source error")
	}

	var c = []string{
		"foo",
		"fred",
	}

	db, _ := Open("dummy", c...)

	if numConns := len(db.cpool.pool); numConns == 0 {
		t.Errorf("expect at least %d db connections, got %d", 1, numConns)
	}

	time.Sleep(10 * time.Millisecond)

	if numConns := len(db.cpool.pool); numConns != 2 {
		t.Errorf("expect at least %d db connections, got %d", 2, numConns)
	}
}

func TestSetSticky(t *testing.T) {
	db, _ := Open("dummy", "foo")

	db.SetSticky(false)

	if db.sticky {
		t.Errorf("expect sticky to be set")
	}
}

func TestClone(t *testing.T) {
	db, _ := Open("dummy", "foo")

	dbClone := db.Clone()

	if db.cpool != dbClone.cpool {
		t.Errorf("cloned db should use the same cpool pointer")
	}

	db.SetSticky(false)
	if dbClone.sticky == false {
		t.Errorf("cloned db sticky attribute passed by reference")
	}
}

func TestDriver(t *testing.T) {
	db, _ := Open("dummy", "foo")

	driver := db.Driver()

	if driver != db.cpool.pool[0].Driver() {
		t.Errorf("expect driver taken from the writer, instead got %s", driver)
	}
}

func TestNext(t *testing.T) {
	var c = []string{
		"foo",
		"fred",
	}

	db, _ := Open("dummy", c...)
	time.Sleep(10 * time.Millisecond)

	db.next()
	writer, _ := db.next()

	if writer != db.cpool.pool[0] {
		t.Errorf("expect first next to return a writer, instead got %s", writer)
	}

	reader, _ := db.next()

	if reader != db.cpool.pool[1] {
		t.Errorf("expect reader to return, instead got %s", reader)
	}

	db.modified = true
	db.next()
	writer, _ = db.next()

	if writer != db.cpool.pool[0] {
		t.Errorf("expect writer to return, instead got %s", writer)
	}
}
