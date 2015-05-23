package main

import (
	"database/sql"
	"fmt"
	gopg "github.com/go-pg/pg"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	_ "github.com/lib/pq"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var selectPeopleJSONSQL = `
select coalesce(json_agg(row_to_json(person)), '[]'::json)
from person
where id between $1 and $1 + 25
`

func main() {
	connPoolConfig, err := extractConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "extractConfig failed:", err)
		os.Exit(1)
	}

	err = loadTestData(connPoolConfig)
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

	pg, err := openPg(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "openPg failed: %v", err)
		os.Exit(1)
	}
	pgStmt, err := pg.Prepare(selectPeopleJSONSQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pg.Prepare failed: %v", err)
		os.Exit(1)
	}

	http.HandleFunc("/people/pgx-native", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var json string

		err := pgxPool.QueryRow("selectPeopleJSON", rand.Int31n(10000)).Scan(&json)
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

func extractConfig() (config pgx.ConnPoolConfig, err error) {
	config.ConnConfig, err = pgx.ParseEnvLibpq()
	if err != nil {
		return config, err
	}

	if config.Host == "" {
		config.Host = "localhost"
	}

	if config.User == "" {
		config.User = os.Getenv("USER")
	}

	if config.Database == "" {
		config.Database = "go_db_bench"
	}

	config.TLSConfig = nil
	config.UseFallbackTLS = false

	config.MaxConnections = 10

	return config, nil
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

func openPg(config pgx.ConnPoolConfig) (*gopg.DB, error) {
	var options gopg.Options

	options.Host = config.Host
	_, err := os.Stat(options.Host)
	if err == nil {
		options.Network = "unix"
		if !strings.Contains(options.Host, "/.s.PGSQL.") {
			options.Host = filepath.Join(options.Host, ".s.PGSQL.5432")
		}
	}

	options.User = config.User
	options.Database = config.Database
	options.Password = config.Password

	return gopg.Connect(&options), nil
}
