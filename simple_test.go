package sqlite3

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

func isErr(op s, e error) bool {
	if e == nil {
		return false
	}
	if s != "" {
		s += ": "
	}
	fmt.Printf("DBSimpleTest: %s%s\n", s, e.Error())
	return true
}

// DBSimpleTest requires write permission to the current working
// directory so that it can write a file named foo.db . Rather
// than write errors to logging (as in the original version), it
// writes the error to stdout and returns the error to the caller.
// .
func DBSimpleTest() error {
	var e error
	var db sql.DB
	var tx sql.Transaction

	os.Remove("./foo.db")
	db, e = sql.Open("sqlite3", "./foo.db")
	if isErr("sql.Open", e) {
		return e
	}
	defer db.Close()

	sqlStmt := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, e = db.Exec(sqlStmt)
	if isErr("db.Exec:"+sqlStmt, e) {
		return e
	}
	tx, e = db.Begin()
	if isErr("db.Begin", e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("こんにちは世界%03d", i))
		if isErr(e) {
			return e
		}
		if err != nil {
			// log.Fatal(e) { return e }
			return err
		}
	}
	err = tx.Commit()
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}

	rows, err := db.Query("select id, name from foo")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if isErr(e) {
			return e
		}
		if err != nil {
			// log.Fatal(e) { return e }
			return err
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}

	stmt, err = db.Prepare("select name from foo where id = ?")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	fmt.Println(name)

	_, err = db.Exec("delete from foo")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}

	rows, err = db.Query("select id, name from foo")
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if isErr(e) {
			return e
		}
		if err != nil {
			// log.Fatal(e) { return e }
			return err
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if isErr(e) {
		return e
	}
	if err != nil {
		// log.Fatal(e) { return e }
		return err
	}
	return nil
}
