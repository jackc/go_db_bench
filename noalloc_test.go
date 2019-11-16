package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

func BenchmarkPgConnSelectMultipleRowsWithWithDecodeBinaryZeroAlloc(b *testing.B) {
	setup(b)

	config, err := pgconn.ParseConfig("")
	if err != nil {
		b.Fatalf("pgconn.Connect() failed: %v", err)
	}

	var cr *ChunkReader

	config.BuildFrontend = func(r io.Reader, w io.Writer) pgconn.Frontend {
		cr, err = NewConfig(r, Config{MinBufLen: 64 * 1024})
		if err != nil {
			panic(fmt.Sprintf("BUG: chunkreader.NewConfig failed: %v", err))
		}
		frontend := pgproto3.NewFrontend(cr, w)

		return frontend
	}

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		b.Fatalf("pgconn.Connect() failed: %v", err)
	}
	_, err = pgConn.Prepare(context.Background(), "selectMultiplePeople", selectMultiplePeopleSQL, nil)
	if err != nil {
		b.Fatalf("pgConn.Prepare() failed: %v", err)
	}

	type personRaw struct {
		Id         pgtype.Int4
		FirstName  pgtype.GenericBinary
		LastName   pgtype.GenericBinary
		Sex        pgtype.GenericBinary
		BirthDate  pgtype.Date
		Weight     pgtype.Int4
		Height     pgtype.Int4
		UpdateTime pgtype.Timestamptz
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]

		buf := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(buf, uint32(id))

		rr := pgConn.ExecPrepared(context.Background(), "selectMultiplePeople", [][]byte{buf}, []int16{1}, []int16{1})

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

		cr.Reset()
	}
}
