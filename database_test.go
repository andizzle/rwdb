package rwdb

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/andizzle/go-fakedb"
)

var db *DB

func init() {
	db, _ = Open("fakedb", "foo")
}

func TestOpen(t *testing.T) {
	_, err := Open("fakedb")

	if err == nil {
		t.Errorf("expect no data source error")
	}

	tdb, _ := Open("fakedb", "foo", "fred")

	if numConns := tdb.cpool.poolSize(); numConns == 0 {
		t.Errorf("expect at least %d db connections, got %d", 1, numConns)
	}

	time.Sleep(10 * time.Millisecond)

	if numConns := tdb.cpool.poolSize(); numConns != 2 {
		t.Errorf("expect at least %d db connections, got %d", 2, numConns)
	}
}

func TestSetSticky(t *testing.T) {
	db.SetSticky(false)

	if db.sticky {
		t.Errorf("expect sticky to be set")
	}
}

func TestNew(t *testing.T) {
	db, _ := Open("fakedb", "foo")
	dbClone := db.New()

	if db.cpool != dbClone.cpool {
		t.Errorf("cloned db should use the same cpool pointer")
	}

	db.SetSticky(false)
	if dbClone.sticky == false {
		t.Errorf("cloned db sticky attribute passed by reference")
	}
}

func TestDriver(t *testing.T) {
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

	db, _ := Open("fakedb", c...)
	time.Sleep(10 * time.Millisecond)

	db.next()
	writer, _ := db.next()

	if writer != db.cpool.pool[0] {
		t.Errorf("expect first next to return a writer, instead got %v", writer)
	}

	reader, _ := db.next()

	if reader != db.cpool.pool[1] {
		t.Errorf("expect reader to return, instead got %v", reader)
	}

	db.modified = true
	db.next()
	writer, _ = db.next()

	if writer != db.cpool.pool[0] {
		t.Errorf("expect writer to return, instead got %v", writer)
	}
}

func TestBegin(t *testing.T) {
	_, err := db.Begin()

	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestQuery(t *testing.T) {
	_, err := db.Query("SELECT")

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	tdb, _ := Open("fakedb", "foo")
	tdb.cpool.pool = []*sql.DB{}
	_, err = tdb.Query("SELECT")

	if err.Error() != "no reader db available" {
		t.Errorf("expect no reader db available error, got %v instead", err)
	}
}

func TestQueryRow(t *testing.T) {
	r := db.QueryRow("SELECT")

	switch v := r.(type) {
	case Row:
	default:
		t.Errorf("expect Row, got %T instead", v)
	}

	tdb, _ := Open("fakedb", "foo")
	tdb.cpool.pool = []*sql.DB{}
	r = tdb.QueryRow("SELECT")

	switch v := r.(type) {
	case Row:
	default:
		t.Errorf("expect Row, got %T instead", v)
	}
}

func TestExec(t *testing.T) {
	_, err := db.Exec("INSERT")

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	tdb, _ := Open("fakedb", "foo")
	tdb.cpool.pool = []*sql.DB{}
	_, err = tdb.Query("SELECT")

	if err.Error() != "no reader db available" {
		t.Errorf("expect no reader db available error, got %v instead", err)
	}
}

func TestPrepare(t *testing.T) {
	db, _ := Open("fakedb", "foo", "fred")

	time.Sleep(10 * time.Millisecond)

	_, err := db.Prepare("INSERT")
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	db.cpool.pool = []*sql.DB{}

	_, err = db.Prepare("INSERT")
	if err.Error() != "no writer db available" {
		t.Errorf("expect no writer db available error, got %v instead", err)
	}
}

func TestSetConnMaxLifetime(t *testing.T) {
	db, _ := Open("fakedb", "foo")
	max := 1000 * time.Millisecond
	db.SetConnMaxLifetime(max)

	if db.maxLifetime != max {
		t.Errorf("expect db conn max life time to be %v, got %v instead", max, db.maxLifetime)
	}
}

func TestSetMaxOpenConns(t *testing.T) {
	db, _ := Open("fakedb", "foo")
	db.SetMaxOpenConns(2)

	if db.maxOpen != 2 {
		t.Errorf("expect db conn max life time to be %v, got %v instead", 2, db.maxOpen)
	}
}

func TestSetMaxIdleConns(t *testing.T) {
	db, _ := Open("fakedb", "foo")
	db.SetMaxIdleConns(2)

	if db.maxIdle != 2 {
		t.Errorf("expect db conn max life time to be %v, got %v instead", 2, db.maxIdle)
	}
}

func TestClose(t *testing.T) {
	db, _ := Open("fakedb", "foo")

	db.Close()
}

func TestPing(t *testing.T) {
	db, _ := Open("fakedb", "foo")

	err := db.Ping()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}
