// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db_holder

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value

	db            atomic.Pointer[sql.DB]
	dbRefreshTime time.Time

	dbMux sync.Mutex

	DBConnErrCount = NewReConnectionBackOff(time.Minute, DBConnRetryThreshold)
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const DBRefreshTime = time.Hour

type DBUser struct {
	UserName string
	Password string
}

func SetSQLWriterDBUser(userName string, password string) {
	user := &DBUser{
		UserName: userName,
		Password: password,
	}
	sqlWriterDBUser.Store(user)
}
func GetSQLWriterDBUser() (*DBUser, error) {
	dbUser := sqlWriterDBUser.Load()
	if dbUser == nil {
		return nil, errNotReady
	} else {
		return sqlWriterDBUser.Load().(*DBUser), nil

	}
}

func SetSQLWriterDBAddressFunc(f func(context.Context, bool) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context, bool) (string, error) {
	if f := dbAddressFunc.Load(); f == nil {
		return nil
	} else {
		return f.(func(context.Context, bool) (string, error))
	}
}

func SetDBConn(conn *sql.DB) {
	db.Store(conn)
	dbRefreshTime = time.Now().Add(DBRefreshTime)
}

func CloseDBConn() {
	dbConn := db.Load()
	if dbConn != nil {
		dbConn.Close()
	}
}

// GetOrInitDBConn get the target cn to do the load query.
// if @forceNewConn is true, it will force close old db conn, and re-find new cn.
// if @forceNewConn is false, it will normally fetch the current db connection. BUT in 1 cases, it will re-find the cn:
//  1. DBRefreshTime interval, it will try to fetch the 'new' ob-sys cn.
var GetOrInitDBConn = func(forceNewConn bool, randomCN bool) (*sql.DB, error) {
	dbMux.Lock()
	defer dbMux.Unlock()
	initFunc := func() error {
		CloseDBConn()
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			return errNotReady
		}

		// TODO: trigger with new selected-CN, converge all connections
		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			return errNotReady
		}
		dbAddress, err := addressFunc(context.Background(), randomCN)
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=10s&writeTimeout=15s&timeout=15s&maxAllowedPacket=0&disable_txn_trace=1",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		newDBConn, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}

		//45s suggest by xzxiong
		newDBConn.SetConnMaxLifetime(45 * time.Second)
		newDBConn.SetMaxOpenConns(MaxConnectionNumber)
		newDBConn.SetMaxIdleConns(MaxConnectionNumber)
		SetDBConn(newDBConn)
		return nil
	}

	if forceNewConn || db.Load() == nil {
		err := initFunc()
		if err != nil {
			return nil, err
		}
	} else if time.Now().After(dbRefreshTime) {
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}

	return db.Load(), nil
}

func WriteRowRecords(records [][]string, tbl *table.Table, timeout time.Duration) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error

	var dbConn *sql.DB

	if !DBConnErrCount.Check() {
		dbConn, err = GetOrInitDBConn(true, true)
	} else {
		dbConn, err = GetOrInitDBConn(false, false)
	}
	if err != nil {
		v2.TraceMOLoggerErrorConnDBCounter.Inc()
		_ = DBConnErrCount.Count()
		return 0, err
	}
	if dbConn.Ping() != nil {
		v2.TraceMOLoggerErrorPingDBCounter.Inc()
		_ = DBConnErrCount.Count()
		return 0, err
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseWriteRowRecords)
	defer cancel()

	err = bulkInsert(ctx, dbConn, records, tbl)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		_ = DBConnErrCount.Count()
		return 0, err
	}

	return len(records), nil
}

const initedSize = 4 * mpool.MB

var bufPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, initedSize))
}}

func getBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	if buf != nil {
		buf.Reset()
		bufPool.Put(buf)
	}
}

var _ table.RowWriter = (*CSVWriter)(nil)

type CSVWriter struct {
	ctx       context.Context
	formatter *csv.Writer
	buf       *bytes.Buffer
	release   func(buffer *bytes.Buffer)
}

func NewCSVWriter(ctx context.Context) *CSVWriter {
	buf := getBuffer()
	buf.Reset()
	writer := csv.NewWriter(buf)

	w := &CSVWriter{
		ctx:       ctx,
		buf:       buf,
		formatter: writer,
		release:   putBuffer,
	}
	return w
}

func NewCSVWriterWithBuffer(ctx context.Context, buf *bytes.Buffer) *CSVWriter {
	writer := csv.NewWriter(buf)

	w := &CSVWriter{
		ctx:       ctx,
		buf:       buf,
		formatter: writer,
		release:   nil,
	}
	return w
}
func (w *CSVWriter) WriteRow(row *table.Row) error { return w.WriteStrings(row.ToStrings()) }
func (w *CSVWriter) GetContentLength() int         { w.formatter.Flush(); return w.buf.Len() }

// FlushAndClose implements RowWriter, but NO Close action.
func (w *CSVWriter) FlushAndClose() (int, error) {
	w.formatter.Flush()
	return w.GetContentLength(), nil
}

func (w *CSVWriter) ResetBuffer(buf *bytes.Buffer) {
	w.buf = buf
	w.formatter = csv.NewWriter(buf)
}

func (w *CSVWriter) WriteStrings(record []string) error {
	if err := w.formatter.Write(record); err != nil {
		return err
	}
	return nil
}

func (w *CSVWriter) GetContent() string {
	w.formatter.Flush() // Ensure all data is written to buffer
	return w.buf.String()
}

func (w *CSVWriter) Flush() { w.formatter.Flush() }

func (w *CSVWriter) Release() {
	if w.buf != nil {
		if w.release != nil {
			// fix: need to release the !nil buffer
			defer w.release(w.buf)
		}
		w.buf = nil
		w.formatter = nil
	}
	w.release = nil
}

func bulkInsert(ctx context.Context, sqlDb *sql.DB, records [][]string, tbl *table.Table) error {
	if len(records) == 0 {
		return nil
	}

	csvWriter := NewCSVWriter(ctx)
	defer csvWriter.Release() // Ensures that the buffer is returned to the pool

	// Write each record of the chunk to the CSVWriter
	for _, record := range records {
		for i, col := range record {
			record[i] = strings.ReplaceAll(strings.ReplaceAll(col, "\\", "\\\\"), "'", "''")
		}
		if err := csvWriter.WriteStrings(record); err != nil {
			return err
		}
	}

	csvData := csvWriter.GetContent()

	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s FIELDS TERMINATED BY ','", csvData, tbl.Database, tbl.Table)
	v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(loadSQL)))

	// Use the transaction to execute the SQL command

	_, execErr := sqlDb.Exec(loadSQL)

	return execErr

}

type DBConnProvider func(forceNewConn bool, randomCN bool) (*sql.DB, error)

func IsRecordExisted(ctx context.Context, record []string, tbl *table.Table, getDBConn DBConnProvider) (bool, error) {
	dbConn, err := getDBConn(false, false)
	if err != nil {
		return false, err
	}

	if tbl.Table == "statement_info" {
		const stmtIDIndex = 0           // Replace with actual index for statement ID if different
		const accountIndex = 3          // Replace with actual index for account
		const statusIndex = 15          // Replace with actual index for status
		const requestAtIndex = 12       // Replace with actual index for request_at
		if len(record) <= statusIndex { // Use the largest index you will access
			return false, nil
		}
		return isStatementExisted(ctx, dbConn, record[stmtIDIndex], record[statusIndex], record[requestAtIndex], record[accountIndex])
	}

	return false, nil
}

func isStatementExisted(ctx context.Context, db *sql.DB, stmtId string, status string, request_at, account string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM `system`.statement_info WHERE statement_id = ? AND status = ? AND request_at = ? AND account = ?)"
	err := db.QueryRowContext(ctx, query, stmtId, status, request_at, account).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

var gLabels map[string]string = nil

func SetLabelSelector(labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	gLabels = make(map[string]string, len(labels)+1)
	gLabels["account"] = "sys"
	for k, v := range labels {
		gLabels[k] = v
	}
}

// GetLabelSelector
// Tips: more details in route.RouteForSuperTenant function. It mainly depends on S1.
// Tips: gLabels better contain {"account":"sys"}.
// - Because clusterservice.Selector using clusterservice.globbing do regex-match in route.RouteForSuperTenant
// - If you use labels{"account":"sys", "role":"ob"}, the Selector can match those pods, which have labels{"account":"*", "role":"ob"}
func GetLabelSelector() map[string]string {
	return gLabels
}

var _ table.BackOff = (*ReConnectionBackOff)(nil)

type ReConnectionBackOff struct {
	lock sync.Mutex
	// setting
	window    time.Duration
	threshold int
	// status
	last  time.Time
	count int
}

func NewReConnectionBackOff(window time.Duration, threshold int) *ReConnectionBackOff {
	return &ReConnectionBackOff{
		window:    window,
		threshold: threshold,
		last:      time.Now(),
		count:     0,
	}
}

// Count implement table.BackOff
// return true, means not in backoff cycle. You can run your code.
func (b *ReConnectionBackOff) Count() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	if time.Since(b.last) > b.window {
		b.count = 1
		b.last = time.Now()
		return true
	}

	b.count++
	b.last = time.Now()
	return b.count <= b.threshold
}

// Check return same as Count, but without changed.
func (b *ReConnectionBackOff) Check() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.count <= b.threshold ||
		(time.Now().Before(b.last) || time.Since(b.last) > b.window)
}
