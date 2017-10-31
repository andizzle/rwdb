package rwdb

import (
	"testing"
	"time"
)

func TestStmtClose(t *testing.T) {
	stmtc, _ := db.Prepare("INSERT")
	err := stmtc.Close()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestStmtExec(t *testing.T) {
	stmt := &stmt{}

	_, err := stmt.Exec("INSERT")
	if err.Error() != "zero statement executable" {
		t.Errorf("unexpected error %v", err)
	}

	stmtc, _ := db.Prepare("INSERT")
	_, err = stmtc.Exec()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestStmtQuery(t *testing.T) {
	stmt := &stmt{}

	_, err := stmt.Query("SELECT")
	if err.Error() != "zero statement executable" {
		t.Errorf("unexpected error %v", err)
	}

	stmtc, _ := db.Prepare("SELECT")
	_, err = stmtc.Query()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	dbt, _ := Open("fakedb", "foo", "fred")
	time.Sleep(10 * time.Millisecond)

	stmtc, _ = dbt.Prepare("SELECT")
	time.Sleep(10 * time.Millisecond)

	_, err = stmtc.Query()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}
