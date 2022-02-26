package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	gopg "github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"github.com/go-pg/pg/v10/types"
	"github.com/jackc/go_db_bench/raw"
	pgconnv4 "github.com/jackc/pgconn"
	stmtcachev4 "github.com/jackc/pgconn/stmtcache"
	pgtypev4 "github.com/jackc/pgtype"
	pgxv4 "github.com/jackc/pgx/v4"
	pgxpoolv4 "github.com/jackc/pgx/v4/pgxpool"
	pgxv5 "github.com/jackc/pgx/v5"
	pgconnv5 "github.com/jackc/pgx/v5/pgconn"
	pgxpoolv5 "github.com/jackc/pgx/v5/pgxpool"
)

var (
	setupOnce     sync.Once
	pgxPoolV4     *pgxpoolv4.Pool
	pgxStdlibV4   *sql.DB
	pgxPoolV5     *pgxpoolv5.Pool
	pq            *sql.DB
	pg            *gopg.DB
	pgConnV4      *pgconnv4.PgConn
	pgConnV5      *pgconnv5.PgConn
	rawConn       *raw.Conn
	randPersonIDs []int32
)

var selectPersonNameSQL = `select first_name from person where id=$1`
var selectPersonNameSQLQuestionMark = `select first_name from person where id=?`

var selectPersonSQL = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id=$1`
var selectPersonSQLQuestionMark = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id=?`

var selectMultiplePeopleSQL = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id between $1 and $1 + 24`
var selectMultiplePeopleSQLQuestionMark = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id between ? and ? + 24`

var selectLargeTextSQL = `select repeat('*', $1)`

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

type personBytes struct {
	Id         int32
	FirstName  []byte
	LastName   []byte
	Sex        []byte
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

// Implements pg.ColumnScanner.
var _ orm.ColumnScanner = (*person)(nil)

func (p *person) ScanColumn(colInfo types.ColumnInfo, rd types.Reader, n int) error {
	var err error
	var n64 int64

	switch colInfo.Name {
	case "id":
		n64, err = types.ScanInt64(rd, n)
		p.Id = int32(n64)
	case "first_name":
		p.FirstName, err = types.ScanString(rd, n)
	case "last_name":
		p.LastName, err = types.ScanString(rd, n)
	case "sex":
		p.Sex, err = types.ScanString(rd, n)
	case "birth_date":
		p.BirthDate, err = types.ScanTime(rd, n)
	case "weight":
		n64, err = types.ScanInt64(rd, n)
		p.Weight = int32(n64)
	case "height":
		n64, err = types.ScanInt64(rd, n)
		p.Weight = int32(n64)
	case "update_time":
		p.UpdateTime, err = types.ScanTime(rd, n)
	default:
		panic(fmt.Sprintf("unsupported column: %s", colInfo.Name))
	}

	return err
}

func setup(b *testing.B) {
	setupOnce.Do(func() {
		configV4, err := pgxpoolv4.ParseConfig("")
		if err != nil {
			b.Fatalf("extractConfig failed: %v", err)
		}

		configV4.AfterConnect = func(ctx context.Context, conn *pgxv4.Conn) error {
			_, err := conn.Prepare(ctx, "selectPersonName", selectPersonNameSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectPerson", selectPersonSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectMultiplePeople", selectMultiplePeopleSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectLargeText", selectLargeTextSQL)
			if err != nil {
				return err
			}

			return nil
		}

		err = loadTestData(configV4.ConnConfig)
		if err != nil {
			b.Fatalf("loadTestData failed: %v", err)
		}

		pgxPoolV4, err = openPgxNativeV4(configV4)
		if err != nil {
			b.Fatalf("openPgxNative failed: %v", err)
		}

		configV5, err := pgxpoolv5.ParseConfig("")
		if err != nil {
			b.Fatalf("extractConfig failed: %v", err)
		}

		configV5.AfterConnect = func(ctx context.Context, conn *pgxv5.Conn) error {
			_, err := conn.Prepare(ctx, "selectPersonName", selectPersonNameSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectPerson", selectPersonSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectMultiplePeople", selectMultiplePeopleSQL)
			if err != nil {
				return err
			}

			_, err = conn.Prepare(ctx, "selectLargeText", selectLargeTextSQL)
			if err != nil {
				return err
			}

			return nil
		}

		pgxPoolV5, err = openPgxNativeV5(configV5)
		if err != nil {
			b.Fatalf("openPgxNative failed: %v", err)
		}

		pgxStdlibV4, err = openPgxStdlibV4(configV4)
		if err != nil {
			b.Fatalf("openPgxNative failed: %v", err)
		}

		pq, err = openPq(configV4.ConnConfig)
		if err != nil {
			b.Fatalf("openPq failed: %v", err)
		}

		pg, err = openPg(*configV4.ConnConfig)
		if err != nil {
			b.Fatalf("openPg failed: %v", err)
		}

		pgConnV4, err = pgconnv4.Connect(context.Background(), "")
		if err != nil {
			b.Fatalf("pgconn.Connect() failed: %v", err)
		}
		_, err = pgConnV4.Prepare(context.Background(), "selectPerson", selectPersonSQL, nil)
		if err != nil {
			b.Fatalf("pgConn.Prepare() failed: %v", err)
		}
		_, err = pgConnV4.Prepare(context.Background(), "selectMultiplePeople", selectMultiplePeopleSQL, nil)
		if err != nil {
			b.Fatalf("pgConn.Prepare() failed: %v", err)
		}

		pgConnV5, err = pgconnv5.Connect(context.Background(), "")
		if err != nil {
			b.Fatalf("pgconn.Connect() failed: %v", err)
		}
		_, err = pgConnV5.Prepare(context.Background(), "selectPerson", selectPersonSQL, nil)
		if err != nil {
			b.Fatalf("pgConn.Prepare() failed: %v", err)
		}
		_, err = pgConnV5.Prepare(context.Background(), "selectMultiplePeople", selectMultiplePeopleSQL, nil)
		if err != nil {
			b.Fatalf("pgConn.Prepare() failed: %v", err)
		}

		rawConfig := raw.ConnConfig{
			Host:     configV4.ConnConfig.Host,
			Port:     configV4.ConnConfig.Port,
			User:     configV4.ConnConfig.User,
			Password: configV4.ConnConfig.Password,
			Database: configV4.ConnConfig.Database,
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
		rows, _ := pgxPoolV4.Query(context.Background(), "select id from person order by random()")
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

func BenchmarkPgxV4NativeSelectSingleShortString(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName string
		err := pgxPoolV4.QueryRow(context.Background(), "selectPersonName", id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxV5NativeSelectSingleShortString(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName string
		err := pgxPoolV5.QueryRow(context.Background(), "selectPersonName", id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPoolV5.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxV4StdlibSelectSingleShortString(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleShortString(b, stmt)
}

func BenchmarkPgSelectSingleShortString(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var firstName string
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.QueryOne(gopg.Scan(&firstName), id)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPqSelectSingleShortString(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleShortString(b, stmt)
}

func benchmarkSelectSingleShortString(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
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

func BenchmarkRawSelectSingleShortValue(b *testing.B) {
	setup(b)

	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonNameStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildQueryBuf failed: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func BenchmarkPgxV4NativeSelectSingleShortBytes(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName []byte
		err := pgxPoolV4.QueryRow(context.Background(), "selectPersonName", id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxV5NativeSelectSingleShortBytes(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName []byte
		err := pgxPoolV5.QueryRow(context.Background(), "selectPersonName", id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPoolV5.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxV4StdlibSelectSingleShortBytes(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleShortBytes(b, stmt)
}

func BenchmarkPqSelectSingleShortBytes(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleShortBytes(b, stmt)
}

func benchmarkSelectSingleShortBytes(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := stmt.QueryRow(id)
		var firstName []byte
		err := row.Scan(&firstName)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
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

func BenchmarkPgxV4NativeSelectSingleRow(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV4.Query(context.Background(), "selectPerson", id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxV4NativeSelectSingleRowNotPreparedWithStatementCacheModePrepare(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = func(conn *pgconnv4.PgConn) stmtcachev4.Cache {
		return stmtcachev4.New(conn, stmtcachev4.ModePrepare, 512)
	}

	db, err := openPgxNativeV4(config)
	if err != nil {
		b.Fatalf("openPgxNative failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := db.Query(context.Background(), selectPersonSQL, id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxV4NativeSelectSingleRowNotPreparedWithStatementCacheModeDescribe(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = func(conn *pgconnv4.PgConn) stmtcachev4.Cache {
		return stmtcachev4.New(conn, stmtcachev4.ModeDescribe, 512)
	}

	db, err := openPgxNativeV4(config)
	if err != nil {
		b.Fatalf("openPgxNative failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := db.Query(context.Background(), selectPersonSQL, id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxV4NativeSelectSingleRowNotPreparedWithStatementCacheDisabled(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = nil

	db, err := openPgxNativeV4(config)
	if err != nil {
		b.Fatalf("openPgxNative failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := db.Query(context.Background(), selectPersonSQL, id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxV5NativeSelectSingleRow(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV5.Query(context.Background(), "selectPerson", id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPoolV5.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgConnV4SelectSingleRowTextProtocolNoParsing(b *testing.B) {
	setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConnV4.ExecPrepared(context.Background(), "selectPerson", [][]byte{buf}, []int16{1}, nil)
		_, err := rr.Close()
		if err != nil {
			b.Fatalf("pgConn.ExecPrepared failed: %v", err)
		}
	}
}

func BenchmarkPgConnV5SelectSingleRowTextProtocolNoParsing(b *testing.B) {
	setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConnV5.ExecPrepared(context.Background(), "selectPerson", [][]byte{buf}, []int16{1}, nil)
		_, err := rr.Close()
		if err != nil {
			b.Fatalf("pgConnV5.ExecPrepared failed: %v", err)
		}
	}
}

func BenchmarkPgConnV4SelectSingleRowBinaryProtocolNoParsing(b *testing.B) {
	setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConnV4.ExecPrepared(context.Background(), "selectPerson", [][]byte{buf}, []int16{1}, []int16{1})
		_, err := rr.Close()
		if err != nil {
			b.Fatalf("pgConn.ExecPrepared failed: %v", err)
		}
	}
}

func BenchmarkPgConnV5SelectSingleRowBinaryProtocolNoParsing(b *testing.B) {
	setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConnV5.ExecPrepared(context.Background(), "selectPerson", [][]byte{buf}, []int16{1}, []int16{1})
		_, err := rr.Close()
		if err != nil {
			b.Fatalf("pgConnV5.ExecPrepared failed: %v", err)
		}
	}
}

func BenchmarkPgxV4StdlibSelectSingleRow(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleRow(b, stmt)
}

func BenchmarkPgxV4StdlibSelectSingleRowNotPreparedStatementCacheModePrepare(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = func(conn *pgconnv4.PgConn) stmtcachev4.Cache {
		return stmtcachev4.New(conn, stmtcachev4.ModePrepare, 512)
	}

	pgxStdlibV4, err = openPgxStdlibV4(config)
	if err != nil {
		b.Fatalf("openPgxStdlib failed: %v", err)
	}

	benchmarkSelectSingleRowNotPrepared(b, pgxStdlibV4, selectPersonSQL)
}

func BenchmarkPgxV4StdlibSelectSingleRowNotPreparedStatementCacheModeDescribe(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = func(conn *pgconnv4.PgConn) stmtcachev4.Cache {
		return stmtcachev4.New(conn, stmtcachev4.ModeDescribe, 512)
	}

	pgxStdlibV4, err = openPgxStdlibV4(config)
	if err != nil {
		b.Fatalf("openPgxStdlib failed: %v", err)
	}

	benchmarkSelectSingleRowNotPrepared(b, pgxStdlibV4, selectPersonSQL)
}

func BenchmarkPgxV4StdlibSelectSingleRowNotPreparedStatementCacheModeDisabled(b *testing.B) {
	setup(b)

	config, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		b.Fatalf("ParseConfig failed: %v", err)
	}
	config.ConnConfig.BuildStatementCache = nil

	pgxStdlibV4, err = openPgxStdlibV4(config)
	if err != nil {
		b.Fatalf("openPgxStdlib failed: %v", err)
	}

	benchmarkSelectSingleRowNotPrepared(b, pgxStdlibV4, selectPersonSQL)
}

func BenchmarkPgSelectSingleRow(b *testing.B) {
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

func BenchmarkPgSelectSingleRowNotPrepared(b *testing.B) {
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

func BenchmarkPqSelectSingleRow(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleRow(b, stmt)
}

func BenchmarkPqSelectSingleRowNotPrepared(b *testing.B) {
	setup(b)
	benchmarkSelectSingleRowNotPrepared(b, pq, selectPersonSQL)
}

func benchmarkSelectSingleRow(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
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

func benchmarkSelectSingleRowNotPrepared(b *testing.B, db *sql.DB, sql string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := db.QueryRow(sql, id)
		var p person
		err := row.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkRawSelectSingleRow(b *testing.B) {
	setup(b)
	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func BenchmarkPgxV4NativeSelectMultipleRows(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV4.Query(context.Background(), "selectMultiplePeople", id)
		var p person
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonWasFilled(b, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgxV5NativeSelectMultipleRows(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV5.Query(context.Background(), "selectMultiplePeople", id)
		var p person
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonWasFilled(b, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgConnV4SelectMultipleRowsWithWithDecodeBinary(b *testing.B) {
	setup(b)

	type personRaw struct {
		Id         pgtypev4.Int4
		FirstName  pgtypev4.GenericBinary
		LastName   pgtypev4.GenericBinary
		Sex        pgtypev4.GenericBinary
		BirthDate  pgtypev4.Date
		Weight     pgtypev4.Int4
		Height     pgtypev4.Int4
		UpdateTime pgtypev4.Timestamptz
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConnV4.ExecPrepared(context.Background(), "selectMultiplePeople", [][]byte{buf}, []int16{1}, []int16{1})

		var p personRaw
		for rr.NextRow() {
			var err error
			vi := 0

			err = p.Id.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.FirstName.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.LastName.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Sex.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.BirthDate.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Weight.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Height.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.UpdateTime.DecodeBinary(nil, rr.Values()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}

		}

		_, err := rr.Close()
		if err != nil {
			b.Fatalf("pgConn.ExecPrepared failed: %v", err)
		}
	}
}

func BenchmarkPgxV4NativeSelectMultipleRowsWithoutScan(b *testing.B) {
	setup(b)

	type personRaw struct {
		Id         pgtypev4.Int4
		FirstName  pgtypev4.GenericBinary
		LastName   pgtypev4.GenericBinary
		Sex        pgtypev4.GenericBinary
		BirthDate  pgtypev4.Date
		Weight     pgtypev4.Int4
		Height     pgtypev4.Int4
		UpdateTime pgtypev4.Timestamptz
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV4.Query(context.Background(), "selectMultiplePeople", id)
		var p personRaw
		for rows.Next() {
			var err error
			vi := 0

			err = p.Id.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.FirstName.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.LastName.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Sex.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.BirthDate.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Weight.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Height.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.UpdateTime.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgxV4NativeSelectMultipleRowsIntoGenericBinaryWithoutScan(b *testing.B) {
	setup(b)

	type personRaw struct {
		Id         pgtypev4.GenericBinary
		FirstName  pgtypev4.GenericBinary
		LastName   pgtypev4.GenericBinary
		Sex        pgtypev4.GenericBinary
		BirthDate  pgtypev4.GenericBinary
		Weight     pgtypev4.GenericBinary
		Height     pgtypev4.GenericBinary
		UpdateTime pgtypev4.GenericBinary
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV4.Query(context.Background(), "selectMultiplePeople", id)
		var p personRaw
		for rows.Next() {
			var err error
			vi := 0

			err = p.Id.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.FirstName.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.LastName.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Sex.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.BirthDate.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Weight.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.Height.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
			vi += 1

			err = p.UpdateTime.DecodeBinary(nil, rows.RawValues()[vi])
			if err != nil {
				b.Fatalf("DecodeBinary failed: %v", err)
			}
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgxV4StdlibSelectMultipleRows(b *testing.B) {
	setup(b)

	stmt, err := pgxStdlibV4.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRows(b, stmt)
}

// This benchmark is different than the other multiple rows in that it collects
// all rows where as the others process and discard. So it is not apples-to-
// apples for *SelectMultipleRows*.
func BenchmarkPgSelectMultipleRowsCollect(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.Query(&people, id)
		if err != nil {
			b.Fatalf("stmt.Query failed: %v", err)
		}

		for i := range people {
			checkPersonWasFilled(b, people[i])
		}
	}
}

func BenchmarkPgSelectMultipleRowsAndDiscard(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.Query(gopg.Discard, id)
		if err != nil {
			b.Fatalf("stmt.Query failed: %v", err)
		}
	}
}

func BenchmarkPqSelectMultipleRows(b *testing.B) {
	setup(b)

	stmt, err := pq.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRows(b, stmt)
}

func benchmarkSelectMultipleRows(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		rows, err := stmt.Query(id)
		if err != nil {
			b.Fatalf("db.Query failed: %v", err)
		}

		var p person
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonWasFilled(b, p)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err() returned an error: %v", err)
		}
	}
}

func BenchmarkRawSelectMultipleRows(b *testing.B) {
	setup(b)

	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectMultiplePeopleStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

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

func checkPersonBytesWasFilled(b *testing.B, p personBytes) {
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

func BenchmarkPgxV4NativeSelectMultipleRowsBytes(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV4.Query(context.Background(), "selectMultiplePeople", id)
		var p personBytes
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonBytesWasFilled(b, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgxV5NativeSelectMultipleRowsBytes(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPoolV5.Query(context.Background(), "selectMultiplePeople", id)
		var p personBytes
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonBytesWasFilled(b, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}
	}
}

func BenchmarkPgxV4StdlibSelectMultipleRowsBytes(b *testing.B) {
	setup(b)

	stmt, err := pgxStdlibV4.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRowsBytes(b, stmt)
}

func BenchmarkPqSelectMultipleRowsBytes(b *testing.B) {
	setup(b)

	stmt, err := pq.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRowsBytes(b, stmt)
}

func benchmarkSelectMultipleRowsBytes(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		rows, err := stmt.Query(id)
		if err != nil {
			b.Fatalf("db.Query failed: %v", err)
		}

		var p personBytes
		for rows.Next() {
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			checkPersonBytesWasFilled(b, p)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err() returned an error: %v", err)
		}
	}
}

func BenchmarkPgxV4NativeSelectBatch3Query(b *testing.B) {
	setup(b)

	b.ResetTimer()
	batch := &pgxv4.Batch{}
	results := make([]string, 3)
	for j := range results {
		batch.Queue("selectLargeText", j)
	}

	for i := 0; i < b.N; i++ {
		br := pgxPoolV4.SendBatch(context.Background(), batch)

		for j := range results {
			if err := br.QueryRow().Scan(&results[j]); err != nil {
				b.Fatal(err)
			}
		}

		if err := br.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPgxV5NativeSelectBatch3Query(b *testing.B) {
	setup(b)

	b.ResetTimer()
	batch := &pgxv5.Batch{}
	results := make([]string, 3)
	for j := range results {
		batch.Queue("selectLargeText", j)
	}

	for i := 0; i < b.N; i++ {
		br := pgxPoolV5.SendBatch(context.Background(), batch)

		for j := range results {
			if err := br.QueryRow().Scan(&results[j]); err != nil {
				b.Fatal(err)
			}
		}

		if err := br.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPgxV4NativeSelectNoBatch3Query(b *testing.B) {
	setup(b)

	b.ResetTimer()
	results := make([]string, 3)
	for i := 0; i < b.N; i++ {
		for j := range results {
			if err := pgxPoolV4.QueryRow(context.Background(), "selectLargeText", j).Scan(&results[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkPgxV5NativeSelectNoBatch3Query(b *testing.B) {
	setup(b)

	b.ResetTimer()
	results := make([]string, 3)
	for i := 0; i < b.N; i++ {
		for j := range results {
			if err := pgxPoolV5.QueryRow(context.Background(), "selectLargeText", j).Scan(&results[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkPgxV4StdlibSelectNoBatch3Query(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectNoBatch3Query(b, stmt)
}

func BenchmarkPqSelectNoBatch3Query(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectNoBatch3Query(b, stmt)
}

func benchmarkSelectNoBatch3Query(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	results := make([]string, 3)
	for i := 0; i < b.N; i++ {
		for j := range results {
			if err := stmt.QueryRow(j).Scan(&results[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkPgxV4NativeSelectLargeTextString1KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextString(b, 1024)
}

func BenchmarkPgxV4NativeSelectLargeTextString8KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextString(b, 8*1024)
}

func BenchmarkPgxV4NativeSelectLargeTextString64KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextString(b, 64*1024)
}

func benchmarkPgxV4NativeSelectLargeTextString(b *testing.B, size int) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s string
		err := pgxPoolV4.QueryRow(context.Background(), "selectLargeText", size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPgxV5NativeSelectLargeTextString1KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextString(b, 1024)
}

func BenchmarkPgxV5NativeSelectLargeTextString8KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextString(b, 8*1024)
}

func BenchmarkPgxV5NativeSelectLargeTextString64KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextString(b, 64*1024)
}

func benchmarkPgxV5NativeSelectLargeTextString(b *testing.B, size int) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s string
		err := pgxPoolV5.QueryRow(context.Background(), "selectLargeText", size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPgxV4StdlibSelectLargeTextString1KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextString(b, 1024)
}

func BenchmarkPgxV4StdlibSelectLargeTextString8KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextString(b, 8*1024)
}

func BenchmarkPgxV4StdlibSelectLargeTextString64KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextString(b, 64*1024)
}

func benchmarkPgxV4StdlibSelectLargeTextString(b *testing.B, size int) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectLargeTextString(b, stmt, size)
}

func BenchmarkPgSelectLargeTextString1KB(b *testing.B) {
	benchmarkPgSelectLargeTextString(b, 1024)
}

func BenchmarkPgSelectLargeTextString8KB(b *testing.B) {
	benchmarkPgSelectLargeTextString(b, 8*1024)
}

func BenchmarkPgSelectLargeTextString64KB(b *testing.B) {
	benchmarkPgSelectLargeTextString(b, 64*1024)
}

func benchmarkPgSelectLargeTextString(b *testing.B, size int) {
	setup(b)

	stmt, err := pg.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s string
		_, err := stmt.QueryOne(gopg.Scan(&s), size)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPqSelectLargeTextString1KB(b *testing.B) {
	benchmarkPqSelectLargeTextString(b, 1024)
}

func BenchmarkPqSelectLargeTextString8KB(b *testing.B) {
	benchmarkPqSelectLargeTextString(b, 8*1024)
}

func BenchmarkPqSelectLargeTextString64KB(b *testing.B) {
	benchmarkPqSelectLargeTextString(b, 64*1024)
}

func benchmarkPqSelectLargeTextString(b *testing.B, size int) {
	setup(b)
	stmt, err := pq.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectLargeTextString(b, stmt, size)
}

func benchmarkSelectLargeTextString(b *testing.B, stmt *sql.Stmt, size int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s string
		err := stmt.QueryRow(size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPgxV4NativeSelectLargeTextBytes1KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextBytes(b, 1024)
}

func BenchmarkPgxV4NativeSelectLargeTextBytes8KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextBytes(b, 8*1024)
}

func BenchmarkPgxV4NativeSelectLargeTextBytes64KB(b *testing.B) {
	benchmarkPgxV4NativeSelectLargeTextBytes(b, 64*1024)
}

func benchmarkPgxV4NativeSelectLargeTextBytes(b *testing.B, size int) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s []byte
		err := pgxPoolV4.QueryRow(context.Background(), "selectLargeText", size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPgxV5NativeSelectLargeTextBytes1KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextBytes(b, 1024)
}

func BenchmarkPgxV5NativeSelectLargeTextBytes8KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextBytes(b, 8*1024)
}

func BenchmarkPgxV5NativeSelectLargeTextBytes64KB(b *testing.B) {
	benchmarkPgxV5NativeSelectLargeTextBytes(b, 64*1024)
}

func benchmarkPgxV5NativeSelectLargeTextBytes(b *testing.B, size int) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s []byte
		err := pgxPoolV5.QueryRow(context.Background(), "selectLargeText", size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}

func BenchmarkPgxV4StdlibSelectLargeTextBytes1KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextBytes(b, 1024)
}

func BenchmarkPgxV4StdlibSelectLargeTextBytes8KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextBytes(b, 8*1024)
}

func BenchmarkPgxV4StdlibSelectLargeTextBytes64KB(b *testing.B) {
	benchmarkPgxV4StdlibSelectLargeTextBytes(b, 64*1024)
}

func benchmarkPgxV4StdlibSelectLargeTextBytes(b *testing.B, size int) {
	setup(b)
	stmt, err := pgxStdlibV4.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectLargeTextBytes(b, stmt, size)
}

func BenchmarkPqSelectLargeTextBytes1KB(b *testing.B) {
	benchmarkPqSelectLargeTextBytes(b, 1024)
}

func BenchmarkPqSelectLargeTextBytes8KB(b *testing.B) {
	benchmarkPqSelectLargeTextBytes(b, 8*1024)
}

func BenchmarkPqSelectLargeTextBytes64KB(b *testing.B) {
	benchmarkPqSelectLargeTextBytes(b, 64*1024)
}

func benchmarkPqSelectLargeTextBytes(b *testing.B, size int) {
	setup(b)
	stmt, err := pq.Prepare(selectLargeTextSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectLargeTextBytes(b, stmt, size)
}

func benchmarkSelectLargeTextBytes(b *testing.B, stmt *sql.Stmt, size int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s []byte
		err := stmt.QueryRow(size).Scan(&s)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(s) != size {
			b.Fatalf("expected length %v, got %v", size, len(s))
		}
	}
}
