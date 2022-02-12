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

	gopg "github.com/go-pg/pg/v10"
	pgxv4 "github.com/jackc/pgx/v4"
	pgxpoolv4 "github.com/jackc/pgx/v4/pgxpool"
	stdlibv4 "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

var selectPeopleJSONSQL = `
select coalesce(json_agg(row_to_json(person)), '[]'::json)
from person
where id between $1 and $1 + 25
`

func main() {
	connPoolConfig, err := pgxpoolv4.ParseConfig("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool.ParseConfig failed:", err)
		os.Exit(1)
	}

	err = loadTestData(connPoolConfig.ConnConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadTestData failed:", err)
		os.Exit(1)
	}

	connPoolConfig.AfterConnect = func(ctx context.Context, conn *pgxv4.Conn) error {
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

func loadTestData(config *pgxv4.ConnConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgxv4.ConnectConfig(ctx, config)
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

func openPgxNative(config *pgxpoolv4.Config) (*pgxpoolv4.Pool, error) {
	return pgxpoolv4.ConnectConfig(context.Background(), config)
}

func openPgxStdlib(config *pgxpoolv4.Config) (*sql.DB, error) {
	db := stdlibv4.OpenDB(*config.ConnConfig)
	return db, db.Ping()
}

func openPq(config *pgxv4.ConnConfig) (*sql.DB, error) {
	var options []string
	options = append(options, fmt.Sprintf("host=%s", config.Host))
	options = append(options, fmt.Sprintf("port=%v", config.Port))
	options = append(options, fmt.Sprintf("user=%s", config.User))
	options = append(options, fmt.Sprintf("dbname=%s", config.Database))
	if config.TLSConfig == nil {
		options = append(options, "sslmode=disable")
	} else {
		options = append(options, "sslmode=require")
	}
	if config.Password != "" {
		options = append(options, fmt.Sprintf("password=%s", config.Password))
	}

	return sql.Open("postgres", strings.Join(options, " "))
}

func openPg(config pgxv4.ConnConfig) (*gopg.DB, error) {
	var options gopg.Options

	options.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	_, err := os.Stat(config.Host)
	if err == nil {
		options.Network = "unix"
		if !strings.Contains(config.Host, "/.s.PGSQL.") {
			options.Addr = filepath.Join(config.Host, ".s.PGSQL.5432")
		}
	}

	options.User = config.User
	options.Database = config.Database
	options.Password = config.Password
	options.TLSConfig = config.TLSConfig

	return gopg.Connect(&options), nil
}
