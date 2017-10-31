# rwdb

[![Build Status](https://travis-ci.org/andizzle/rwdb.svg?branch=master)](https://travis-ci.org/andizzle/rwdb)
[![GoDoc](https://godoc.org/github.com/andizzle/rwdb?status.svg)](https://godoc.org/github.com/andizzle/rwdb)
[![Coverage Status](https://coveralls.io/repos/github/andizzle/rwdb/badge.svg?branch=master)](https://coveralls.io/github/andizzle/rwdb?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/andizzle/rwdb)](https://goreportcard.com/report/github.com/andizzle/rwdb)

Database wrapper that manage read write connections

## Install

```
go get github.com/andizzle/rwdb
```

## Create connections

```go
package main

import "github.com/andizzle/rwdb"

var conns = []string{
        "tcp://user:pass@write/dbname",
        "tcp://user:pass@read1/dbname",
        "tcp://user:pass@read2/dbname",
        "tcp://user:pass@read3/dbname",
}


// unable to open write will cause an error
db, err := rwdb.Open("driver", conns...)
```

## Rotation read and Sticky read

Query a database rotate the use of database connections

```go
db, err := rwdb.Open("driver", conns...)

// Use the first connection
db.QueryContext(ctx)

// Use the next connection
db.QueryContext(ctx)
```

Execute a statement will cause all subsequent queries to use the write connection (sticky connection). This is to allow the 
immediate reading of records that have been written to the database during the current request cycle. 


```go
db, err := rwdb.Open("driver", conns...)

// Use the next connection
db.QueryContext(ctx)

// Use the write conenction
db.ExecContext(ctx)

// Use the write connection
db.Query()
```

Sticky can be turned off
```go
db.SetSticky(false)
```

The db is marked as modified if there's a successful `Write` to the databse, which turns on the sticky logic. 
However, the real world usecase would require `modified` value to be reset on each request session.

Here's what we can do:

```go
db, err := rwdb.Open("driver", conns...)

func RecordUserLogin() {
        d := db.New()      // This will make sure the following read are not affected by 
                             // other sessions' write action

        d.Query("SELECT * from `users` where id = ?")
        ...
        d.Exec("UPDATE `users` set last_login_at = now();")
        d.Query(...)         // Connection is set to the writer
}
```

## License

[License](https://github.com/andizzle/rwdb/blob/master/LICENSE)
