package main

import (
	"database/sql"
	gopg "github.com/go-pg/pg"
	"github.com/jackc/go_db_bench/raw"
	"github.com/jackc/pgx"
	"sync"
	"testing"
	"time"
)

var (
	setupOnce     sync.Once
	pgxPool       *pgx.ConnPool
	pgxStdlib     *sql.DB
	pq            *sql.DB
	pg            *gopg.DB
	rawConn       *raw.Conn
	randPersonIDs []int32
)

var selectPersonNameSQL = `select first_name from person where id=$1`
var selectPersonNameSQLQuestionMark = `select first_name from person where id=?`

var selectPersonSQL = `
select id, first_name, last_name, Sex, birth_date, Weight, Height, update_time
from person
where id=$1`
var selectPersonSQLQuestionMark = `
select id, first_name, last_name, Sex, birth_date, Weight, Height, update_time
from person
where id=?`

var selectMultiplePeopleSQL = `
select id, first_name, last_name, Sex, birth_date, Weight, Height, update_time
from person
where id between $1 and $1 + 24`
var selectMultiplePeopleSQLQuestionMark = `
select id, first_name, last_name, Sex, birth_date, Weight, Height, update_time
from person
where id between ? and ? + 24`

var rawSelectPersonNameStmt *raw.PreparedStatement
var rawSelectPersonStmt *raw.PreparedStatement
var rawSelectMultiplePeopleStmt *raw.PreparedStatement

var rxBuf []byte

type person struct {
	Id         int32
	FirstName  string
	LastName   string
	Sex        string
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

type People []*person

func (people *People) New() interface{} {
	u := &person{}
	*people = append(*people, u)
	return u
}

func setup(b *testing.B) {
	setupOnce.Do(func() {
		config := extractConfig()

		config.AfterConnect = func(conn *pgx.Conn) error {
			_, err := conn.Prepare("selectPersonName", selectPersonNameSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare("selectPerson", selectPersonSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare("selectMultiplePeople", selectMultiplePeopleSQL)
			if err != nil {
				return err
			}

			return nil
		}

		err := loadTestData(config)
		if err != nil {
			b.Fatalf("loadTestData failed: %v", err)
		}

		pgxPool, err = openPgxNative(config)
		if err != nil {
			b.Fatalf("openPgxNative failed: %v", err)
		}

		pgxStdlib, err = openPgxStdlib(config)
		if err != nil {
			b.Fatalf("openPgxNative failed: %v", err)
		}

		pq, err = openPq(config)
		if err != nil {
			b.Fatalf("openPq failed: %v", err)
		}

		pg, err = openPg(config)
		if err != nil {
			b.Fatalf("openPg failed: %v", err)
		}

		rawConfig := raw.ConnConfig{
			Host:     config.Host,
			Port:     config.Port,
			User:     config.User,
			Password: config.Password,
			Database: config.Database,
		}
		rawConn, err = raw.Connect(rawConfig)
		if err != nil {
			b.Fatalf("raw.Connect failed: %v", err)
		}
		rawSelectPersonNameStmt, err = rawConn.Prepare("selectPersonName", selectPersonNameSQL)
		if err != nil {
			b.Fatalf("rawConn.Prepare failed: %v", err)
		}
		rawSelectPersonStmt, err = rawConn.Prepare("selectPerson", selectPersonSQL)
		if err != nil {
			b.Fatalf("rawConn.Prepare failed: %v", err)
		}
		rawSelectMultiplePeopleStmt, err = rawConn.Prepare("selectMultiplePeople", selectMultiplePeopleSQL)
		if err != nil {
			b.Fatalf("rawConn.Prepare failed: %v", err)
		}

		rxBuf = make([]byte, 16384)

		// Get random person ids in random order outside of timing
		rows, _ := pgxPool.Query("select id from person order by random()")
		for rows.Next() {
			var id int32
			rows.Scan(&id)
			randPersonIDs = append(randPersonIDs, id)
		}

		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", err)
		}
	})
}

func BenchmarkPgxNativeSelectSingleValueUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectSingleValue(b, selectPersonNameSQL)
}

func benchmarkPgxNativeSelectSingleValue(b *testing.B, sql string) {
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName string
		err := pgxPool.QueryRow(sql, id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxStdlibSelectSingleValueUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectSingleValueUnprepared(b, pgxStdlib)
}

func BenchmarkPgSelectSingleValueUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var person struct {
			FirstName string
		}
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := pg.QueryOne(&person, selectPersonNameSQLQuestionMark, id)
		if err != nil {
			b.Fatalf("pg.QueryOne failed: %v", err)
		}
		if len(person.FirstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPqSelectSingleValueUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectSingleValueUnprepared(b, pq)
}

func benchmarkSelectSingleValueUnprepared(b *testing.B, db *sql.DB) {
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := db.QueryRow(selectPersonNameSQL, id)
		var firstName string
		err := row.Scan(&firstName)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxNativeSelectSingleValuePrepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectSingleValue(b, "selectPersonName")
}

func BenchmarkPgxStdlibSelectSingleValuePrepared(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlib.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	benchmarkSelectSingleValuePrepared(b, stmt)
}

func BenchmarkPgSelectSingleValuePrepared(b *testing.B) {
	setup(b)
	stmt, err := pg.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var person struct {
			FirstName string
		}
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.QueryOne(&person, id)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}
		if len(person.FirstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPqSelectSingleValuePrepared(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	benchmarkSelectSingleValuePrepared(b, stmt)
}

func benchmarkSelectSingleValuePrepared(b *testing.B, stmt *sql.Stmt) {
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := stmt.QueryRow(id)
		var firstName string
		err := row.Scan(&firstName)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkRawSelectSingleValuePrepared(b *testing.B) {
	setup(b)

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonNameStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func BenchmarkPgxNativeSelectSingleRowUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectSingleRow(b, selectPersonSQL)
}

func benchmarkPgxNativeSelectSingleRow(b *testing.B, sql string) {
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPool.Query(sql, id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func checkPersonWasFilled(b *testing.B, p person) {
	if p.Id == 0 {
		b.Fatal("id was 0")
	}
	if len(p.FirstName) == 0 {
		b.Fatal("FirstName was empty")
	}
	if len(p.LastName) == 0 {
		b.Fatal("LastName was empty")
	}
	if len(p.Sex) == 0 {
		b.Fatal("Sex was empty")
	}
	var zeroTime time.Time
	if p.BirthDate == zeroTime {
		b.Fatal("BirthDate was zero time")
	}
	if p.Weight == 0 {
		b.Fatal("Weight was 0")
	}
	if p.Height == 0 {
		b.Fatal("Height was 0")
	}
	if p.UpdateTime == zeroTime {
		b.Fatal("UpdateTime was zero time")
	}
}

func BenchmarkPgxStdlibSelectSingleRowUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectSingleRowUnprepared(b, pgxStdlib)
}

func BenchmarkPgSelectSingleRowUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := pg.QueryOne(&p, selectPersonSQLQuestionMark, id)
		if err != nil {
			b.Fatalf("pg.QueryOne failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPqSelectSingleRowUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectSingleRowUnprepared(b, pq)
}

func benchmarkSelectSingleRowUnprepared(b *testing.B, db *sql.DB) {
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := db.QueryRow(selectPersonSQL, id)
		var p person
		err := row.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxNativeSelectSingleRowPrepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectSingleRow(b, "selectPerson")
}

func BenchmarkPgxStdlibSelectSingleRowPrepared(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlib.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	benchmarkSelectSingleRowPrepared(b, stmt)
}

func BenchmarkPgSelectSingleRowPrepared(b *testing.B) {
	setup(b)
	stmt, err := pg.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.QueryOne(&p, id)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPqSelectSingleRowPrepared(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	benchmarkSelectSingleRowPrepared(b, stmt)
}

func benchmarkSelectSingleRowPrepared(b *testing.B, stmt *sql.Stmt) {
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := stmt.QueryRow(id)
		var p person
		err := row.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkRawSelectSingleRowPrepared(b *testing.B) {
	setup(b)

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func BenchmarkPgxNativeSelectMultipleRowsUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectMultipleRows(b, selectMultiplePeopleSQL)
}

func benchmarkPgxNativeSelectMultipleRows(b *testing.B, sql string) {
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPool.Query(sql, id)
		for rows.Next() {
			var p person
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			people = append(people, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		for _, p := range people {
			checkPersonWasFilled(b, p)
		}
	}
}

func BenchmarkPgxStdlibSelectMultipleRowsUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectMultipleRowsUnprepared(b, pgxStdlib)
}

func BenchmarkPgSelectMultipleRowsUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var people People
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := pg.Query(&people, selectMultiplePeopleSQLQuestionMark, id, id)
		if err != nil {
			b.Fatalf("pg.Query failed: %v", err)
		}

		for _, p := range people {
			checkPersonWasFilled(b, *p)
		}
	}
}

func BenchmarkPqSelectMultipleRowsUnprepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkSelectMultipleRowsUnprepared(b, pq)
}

func benchmarkSelectMultipleRowsUnprepared(b *testing.B, db *sql.DB) {
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]
		rows, err := db.Query(selectMultiplePeopleSQL, id)
		if err != nil {
			b.Fatalf("db.Query failed: %v", err)
		}

		for rows.Next() {
			var p person
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			people = append(people, p)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err() returned an error: %v", err)
		}

		for _, p := range people {
			checkPersonWasFilled(b, p)
		}
	}
}

func BenchmarkPgxNativeSelectMultipleRowsPrepared(b *testing.B) {
	setup(b)
	b.ResetTimer()
	benchmarkPgxNativeSelectMultipleRows(b, "selectMultiplePeople")
}

func BenchmarkPgxStdlibSelectMultipleRowsPrepared(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlib.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()
	b.ResetTimer()
	benchmarkSelectMultipleRowsPrepared(b, stmt)
}

func BenchmarkPgSelectMultipleRowsPrepared(b *testing.B) {
	setup(b)
	stmt, err := pg.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var people People
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.Query(&people, id)
		if err != nil {
			b.Fatalf("stmt.Query failed: %v", err)
		}

		for _, p := range people {
			checkPersonWasFilled(b, *p)
		}
	}
}

func BenchmarkPqSelectMultipleRowsPrepared(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	benchmarkSelectMultipleRowsPrepared(b, stmt)
}

func benchmarkSelectMultipleRowsPrepared(b *testing.B, stmt *sql.Stmt) {
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]
		rows, err := stmt.Query(id)
		if err != nil {
			b.Fatalf("db.Query failed: %v", err)
		}

		for rows.Next() {
			var p person
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			people = append(people, p)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err() returned an error: %v", err)
		}

		for _, p := range people {
			checkPersonWasFilled(b, p)
		}
	}
}

func BenchmarkRawSelectMultipleRowsPrepared(b *testing.B) {
	setup(b)

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectMultiplePeopleStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func rxRawUntilReady(b *testing.B) {
	for {
		n, err := rawConn.Conn().Read(rxBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Read failed: %v", err)
		}
		if rxBuf[n-6] == 'Z' && rxBuf[n-2] == 5 && rxBuf[n-1] == 'I' {
			return
		}
	}
}
