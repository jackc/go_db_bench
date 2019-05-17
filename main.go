package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	gopg "github.com/go-pg/pg"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pool"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

var selectPeopleJSONSQL = `
select coalesce(json_agg(row_to_json(person)), '[]'::json)
from person
where id between $1 and $1 + 25
`

func main() {
	connPoolConfig, err := pool.ParseConfig("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool.ParseConfig failed:", err)
		os.Exit(1)
	}

	err = loadTestData(connPoolConfig.ConnConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadTestData failed:", err)
		os.Exit(1)
	}

	connPoolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Prepare(ctx, "selectPeopleJSON", selectPeopleJSONSQL)
		if err != nil {
			return err
		}
		return nil
	}

	pgxPool, err := openPgxNative(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPgxNative failed:", err)
		os.Exit(1)
	}

	pgxStdlib, err := openPgxStdlib(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPgxNative failed:", err)
		os.Exit(1)
	}
	pgxStmt, err := pgxStdlib.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgxStdlib.Prepare failed:", err)
		os.Exit(1)
	}

	pq, err := openPq(connPoolConfig.ConnConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPq failed:", err)
		os.Exit(1)
	}
	pqStmt, err := pq.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pq.Prepare failed:", err)
		os.Exit(1)
	}

	pg, err := openPg(*connPoolConfig.ConnConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPg failed:", err)
		os.Exit(1)
	}
	pgStmt, err := pg.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pg.Prepare failed:", err)
		os.Exit(1)
	}

	http.HandleFunc("/people/pgx-native", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var json string

		err := pgxPool.QueryRow(context.Background(), "selectPeopleJSON", rand.Int31n(10000)).Scan(&json)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		io.WriteString(w, json)
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

	http.HandleFunc("/people/pg", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var json string

		_, err := pgStmt.QueryOne(&json, rand.Int31n(10000))
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

func loadTestData(config *pgx.ConnConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, personCreateSQL)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, personInsertSQL)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, "analyze person")
	if err != nil {
		return err
	}

	return nil
}

func openPgxNative(config *pool.Config) (*pool.Pool, error) {
	return pool.ConnectConfig(context.Background(), config)
}

func openPgxStdlib(config *pool.Config) (*sql.DB, error) {
	db := stdlib.OpenDB(*config.ConnConfig)
	return db, db.Ping()
}

func openPq(config *pgx.ConnConfig) (*sql.DB, error) {
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

func openPg(config pgx.ConnConfig) (*gopg.DB, error) {
	var options gopg.Options

	options.Addr = config.Host
	_, err := os.Stat(options.Addr)
	if err == nil {
		options.Network = "unix"
		if !strings.Contains(options.Addr, "/.s.PGSQL.") {
			options.Addr = filepath.Join(options.Addr, ".s.PGSQL.5432")
		}

	}

	options.User = config.User
	options.Database = config.Database
	options.Password = config.Password

	return gopg.Connect(&options), nil
}
