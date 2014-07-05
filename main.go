package main

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	_ "github.com/lib/pq"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
)

var selectPeopleJSONSQL = `
select coalesce(json_agg(row_to_json(person)), '[]'::json)
from person
where id between $1 and $1 + 25
`

func main() {
	connPoolConfig := extractConfig()

	err := loadTestData(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadTestData failed:", err)
		os.Exit(1)
	}

	connPoolConfig.AfterConnect = func(conn *pgx.Conn) error {
		_, err := conn.Prepare("selectPeopleJSON", selectPeopleJSONSQL)
		if err != nil {
			return err
		}
		return nil
	}

	pgxPool, err := openPgxNative(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPgxNative failed: %v", err)
		os.Exit(1)
	}

	pgxStdlib, err := openPgxStdlib(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPgxNative failed: %v", err)
		os.Exit(1)
	}
	pgxStmt, err := pgxStdlib.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgxStdlib.Prepare failed: %v", err)
		os.Exit(1)
	}

	pq, err := openPq(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPq failed: %v", err)
		os.Exit(1)
	}
	pqStmt, err := pq.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pq.Prepare failed: %v", err)
		os.Exit(1)
	}

	http.HandleFunc("/people/pgx-native", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		qr, _ := pgxPool.Query("selectPeopleJSON", rand.Int31n(10000))

		for qr.NextRow() {
			var rr pgx.RowReader
			json := rr.ReadString(qr)
			io.WriteString(w, json)
		}
		if qr.Err() != nil {
			fmt.Fprintln(os.Stderr, qr.Err())
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	})

	http.HandleFunc("/people/pgx-stdlib", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		row := pgxStmt.QueryRow(rand.Int31n(10000))
		var json string
		err := row.Scan(&json)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		io.WriteString(w, json)
	})

	http.HandleFunc("/people/pq", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		row := pqStmt.QueryRow(rand.Int31n(10000))
		var json string
		err := row.Scan(&json)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		io.WriteString(w, json)
	})

	fmt.Println("Starting Go DB Bench on localhost:8080")
	err = http.ListenAndServe("localhost:8080", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to start web server: ", err)
		os.Exit(1)
	}
}

func extractConfig() pgx.ConnPoolConfig {
	var config pgx.ConnPoolConfig

	config.Host = os.Getenv("GO_DB_BENCH_PG_HOST")
	if config.Host == "" {
		config.Host = "localhost"
	}

	config.User = os.Getenv("GO_DB_BENCH_PG_USER")
	if config.User == "" {
		config.User = os.Getenv("USER")
	}

	config.Password = os.Getenv("GO_DB_BENCH_PG_PASSWORD")

	config.Database = os.Getenv("GO_DB_BENCH_PG_DATABASE")
	if config.Database == "" {
		config.Database = "go_db_bench"
	}

	config.MaxConnections = 10

	return config
}

func loadTestData(config pgx.ConnPoolConfig) error {
	conn, err := pgx.Connect(config.ConnConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(personCreateSQL)
	if err != nil {
		return err
	}

	_, err = conn.Exec(personInsertSQL)
	if err != nil {
		return err
	}

	_, err = conn.Exec("analyze person")
	if err != nil {
		return err
	}

	return nil
}

func openPgxNative(config pgx.ConnPoolConfig) (*pgx.ConnPool, error) {
	return pgx.NewConnPool(config)
}

func openPgxStdlib(config pgx.ConnPoolConfig) (*sql.DB, error) {
	connPool, err := pgx.NewConnPool(config)
	if err != nil {
		return nil, err
	}

	return stdlib.OpenFromConnPool(connPool)
}

func openPq(config pgx.ConnPoolConfig) (*sql.DB, error) {
	var options []string
	options = append(options, fmt.Sprintf("host=%s", config.Host))
	options = append(options, fmt.Sprintf("user=%s", config.User))
	options = append(options, fmt.Sprintf("dbname=%s", config.Database))
	options = append(options, "sslmode=disable")
	if config.Password != "" {
		options = append(options, fmt.Sprintf("password=%s", config.Password))
	}

	return sql.Open("postgres", strings.Join(options, " "))
}
