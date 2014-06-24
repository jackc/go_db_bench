// Package raw is a modified version of pgx for benchmarking optimal database
// driver performance. It is used to establish a connection, then to send and
// receive raw byte slices with no other processing.
package raw

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
)

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	Host       string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port       uint16 // default: 5432
	Database   string
	User       string // default: OS user name
	Password   string
	MsgBufSize int         // Size of work buffer used for transcoding messages. For optimal performance, it should be large enough to store a single row from any result set. Default: 1024
	TLSConfig  *tls.Config // config for TLS connection -- nil disables TLS
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	Conn               net.Conn      // the underlying TCP or unix domain socket connection
	reader             *bufio.Reader // buffered reader to improve read performance
	wbuf               [1024]byte
	buf                *bytes.Buffer     // work buffer to avoid constant alloc and dealloc
	bufSize            int               // desired size of buf
	Pid                int32             // backend pid
	SecretKey          int32             // key to use to send a cancel query message to the server
	RuntimeParams      map[string]string // parameters that have been reported by the server
	config             ConnConfig        // config used when establishing this connection
	TxStatus           byte
	preparedStatements map[string]*PreparedStatement
	alive              bool
	causeOfDeath       error
}

type PreparedStatement struct {
	Name              string
	FieldDescriptions []FieldDescription
	ParameterOids     []Oid
}

type Notification struct {
	Pid     int32  // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
}

type CommandTag string

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (such as "CREATE TABLE") then it returns 0
func (ct CommandTag) RowsAffected() int64 {
	words := strings.Split(string(ct), " ")
	n, _ := strconv.ParseInt(words[len(words)-1], 10, 64)
	return n
}

// NotSingleRowError is returned when exactly 1 row is expected, but 0 or more than
// 1 row is returned
type NotSingleRowError struct {
	RowCount int64
}

func (e NotSingleRowError) Error() string {
	return fmt.Sprintf("Expected to find 1 row exactly, instead found %d", e.RowCount)
}

// UnexpectedColumnCountError is returned when an unexpected number of columns is
// returned from a Select.
type UnexpectedColumnCountError struct {
	ExpectedCount int16
	ActualCount   int16
}

func (e UnexpectedColumnCountError) Error() string {
	return fmt.Sprintf("Expected result to have %d column(s), instead it has %d", e.ExpectedCount, e.ActualCount)
}

type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

var NotificationTimeoutError = errors.New("Notification Timeout")
var DeadConnError = errors.New("Connection is dead")

// Connect establishes a connection with a PostgreSQL server using config.
// config.Host must be specified. config.User will default to the OS user name.
// Other config fields are optional.
func Connect(config ConnConfig) (c *Conn, err error) {
	c = new(Conn)

	c.config = config

	if c.config.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		c.config.User = user.Username
	}

	if c.config.Port == 0 {
		c.config.Port = 5432
	}
	if c.config.MsgBufSize == 0 {
		c.config.MsgBufSize = 1024
	}

	// See if host is a valid path, if yes connect with a socket
	_, err = os.Stat(c.config.Host)
	if err == nil {
		// For backward compatibility accept socket file paths -- but directories are now preferred
		socket := c.config.Host
		if !strings.Contains(socket, "/.s.PGSQL.") {
			socket = filepath.Join(socket, ".s.PGSQL.") + strconv.FormatInt(int64(c.config.Port), 10)
		}

		c.Conn, err = net.Dial("unix", socket)
		if err != nil {
			return nil, err
		}
	} else {
		c.Conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.config.Host, c.config.Port))
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if c != nil && err != nil {
			c.Conn.Close()
			c.alive = false
		}
	}()

	c.bufSize = c.config.MsgBufSize
	c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	c.RuntimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*PreparedStatement)
	c.alive = true

	if config.TLSConfig != nil {
		if err = c.startTLS(); err != nil {
			return
		}
	}

	c.reader = bufio.NewReader(c.Conn)

	msg := newStartupMessage()
	msg.options["user"] = c.config.User
	if c.config.Database != "" {
		msg.options["database"] = c.config.Database
	}
	if err = c.txStartupMessage(msg); err != nil {
		return
	}

	for {
		var t byte
		var r *MessageReader
		t, r, err = c.rxMsg()
		if err != nil {
			return nil, err
		}

		switch t {
		case backendKeyData:
			c.rxBackendKeyData(r)
		case authenticationX:
			if err = c.rxAuthenticationX(r); err != nil {
				return nil, err
			}
		case readyForQuery:
			c.rxReadyForQuery(r)
			return c, nil
		default:
			if err = c.processContextFreeMsg(t, r); err != nil {
				return nil, err
			}
		}
	}
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close() (err error) {
	if !c.IsAlive() {
		return nil
	}

	err = c.txMsg('X', c.getBuf())
	c.die(errors.New("Closed"))
	return err
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
func (c *Conn) Prepare(name, sql string) (ps *PreparedStatement, err error) {
	// parse
	buf := c.getBuf()
	buf.WriteString(name)
	buf.WriteByte(0)
	buf.WriteString(sql)
	buf.WriteByte(0)
	binary.Write(buf, binary.BigEndian, int16(0))
	err = c.txMsg('P', buf)
	if err != nil {
		return nil, err
	}

	// describe
	buf = c.getBuf()
	buf.WriteByte('S')
	buf.WriteString(name)
	buf.WriteByte(0)
	err = c.txMsg('D', buf)
	if err != nil {
		return nil, err
	}

	// sync
	err = c.txMsg('S', c.getBuf())
	if err != nil {
		return nil, err
	}

	ps = &PreparedStatement{Name: name}

	var softErr error

	for {
		var t byte
		var r *MessageReader
		t, r, err := c.rxMsg()
		if err != nil {
			return nil, err
		}

		switch t {
		case parseComplete:
		case parameterDescription:
			ps.ParameterOids = c.rxParameterDescription(r)
		case rowDescription:
			ps.FieldDescriptions = c.rxRowDescription(r)
			for i := range ps.FieldDescriptions {
				oid := ps.FieldDescriptions[i].DataType
				switch oid {
				// bool, bytea, int8, int2, int4, float4, float8, date, timestamptz
				case 16, 17, 20, 21, 23, 700, 701, 1082, 1184:
					ps.FieldDescriptions[i].FormatCode = 1
				default:
					ps.FieldDescriptions[i].FormatCode = 0
				}
			}
		case noData:
		case readyForQuery:
			c.rxReadyForQuery(r)
			c.preparedStatements[name] = ps
			return ps, softErr
		default:
			if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
				softErr = e
			}
		}
	}
}

func (c *Conn) IsAlive() bool {
	return c.alive
}

func (c *Conn) CauseOfDeath() error {
	return c.causeOfDeath
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Conn) processContextFreeMsg(t byte, r *MessageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	case notificationResponse:
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}
}

func (c *Conn) rxMsg() (t byte, r *MessageReader, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	var body *bytes.Buffer
	if body, err = c.rxMsgBody(bodySize); err != nil {
		return
	}

	r = newMessageReader(body)
	return
}

func (c *Conn) rxMsgHeader() (t byte, bodySize int32, err error) {
	if !c.alive {
		return 0, 0, DeadConnError
	}

	defer func() {
		if err != nil {
			c.die(err)
		}
	}()

	t, err = c.reader.ReadByte()
	if err != nil {
		return
	}
	err = binary.Read(c.reader, binary.BigEndian, &bodySize)
	bodySize -= 4 // remove self from size
	return
}

func (c *Conn) rxMsgBody(bodySize int32) (*bytes.Buffer, error) {
	if !c.alive {
		return nil, DeadConnError
	}

	buf := c.getBuf()
	_, err := io.CopyN(buf, c.reader, int64(bodySize))
	if err != nil {
		c.die(err)
		return nil, err
	}

	return buf, nil
}

func (c *Conn) rxAuthenticationX(r *MessageReader) (err error) {
	code := r.ReadInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		err = c.txPasswordMessage(c.config.Password)
	case 5: // AuthenticationMD5Password
		salt := r.ReadString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.config.Password+c.config.User)+salt)
		err = c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Conn) rxParameterStatus(r *MessageReader) {
	key := r.ReadCString()
	value := r.ReadCString()
	c.RuntimeParams[key] = value
}

func (c *Conn) rxErrorResponse(r *MessageReader) (err PgError) {
	for {
		switch r.ReadByte() {
		case 'S':
			err.Severity = r.ReadCString()
		case 'C':
			err.Code = r.ReadCString()
		case 'M':
			err.Message = r.ReadCString()
		case 0: // End of error message
			return
		default: // Ignore other error fields
			r.ReadCString()
		}
	}
}

func (c *Conn) rxBackendKeyData(r *MessageReader) {
	c.Pid = r.ReadInt32()
	c.SecretKey = r.ReadInt32()
}

func (c *Conn) rxReadyForQuery(r *MessageReader) {
	c.TxStatus = r.ReadByte()
}

func (c *Conn) rxRowDescription(r *MessageReader) (fields []FieldDescription) {
	fieldCount := r.ReadInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.ReadCString()
		f.Table = r.ReadOid()
		f.AttributeNumber = r.ReadInt16()
		f.DataType = r.ReadOid()
		f.DataTypeSize = r.ReadInt16()
		f.Modifier = r.ReadInt32()
		f.FormatCode = r.ReadInt16()
	}
	return
}

func (c *Conn) rxParameterDescription(r *MessageReader) (parameters []Oid) {
	parameterCount := r.ReadInt16()
	parameters = make([]Oid, 0, parameterCount)
	for i := int16(0); i < parameterCount; i++ {
		parameters = append(parameters, r.ReadOid())
	}
	return
}

func (c *Conn) rxCommandComplete(r *MessageReader) string {
	return r.ReadCString()
}

func (c *Conn) startTLS() (err error) {
	err = binary.Write(c.Conn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(c.Conn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		err = errors.New("Could not use TLS")
		return
	}

	c.Conn = tls.Client(c.Conn, c.config.TLSConfig)

	return nil
}

func (c *Conn) txStartupMessage(msg *startupMessage) error {
	_, err := c.Conn.Write(msg.Bytes())
	return err
}

func (c *Conn) txMsg(identifier byte, buf *bytes.Buffer) (err error) {
	if !c.alive {
		return DeadConnError
	}

	defer func() {
		if err != nil {
			c.die(err)
		}
	}()

	err = binary.Write(c.Conn, binary.BigEndian, identifier)
	if err != nil {
		return
	}

	err = binary.Write(c.Conn, binary.BigEndian, int32(buf.Len()+4))
	if err != nil {
		return
	}

	_, err = buf.WriteTo(c.Conn)
	if err != nil {
		return
	}

	return
}

func (c *Conn) txPasswordMessage(password string) (err error) {
	buf := c.getBuf()

	_, err = buf.WriteString(password)
	if err != nil {
		return
	}
	buf.WriteByte(0)
	if err != nil {
		return
	}
	err = c.txMsg('p', buf)
	return
}

// Gets the shared connection buffer. Since bytes.Buffer never releases memory from
// its internal byte array, check on the size and create a new bytes.Buffer so the
// old one can get GC'ed
func (c *Conn) getBuf() *bytes.Buffer {
	c.buf.Reset()
	if cap(c.buf.Bytes()) > c.bufSize {
		c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	}
	return c.buf
}

func (c *Conn) die(err error) {
	c.alive = false
	c.causeOfDeath = err
	c.Conn.Close()
}
