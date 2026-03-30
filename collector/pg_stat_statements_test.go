// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package collector

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func toDriverValues(values []any) []driver.Value {
	dv := make([]driver.Value, len(values))
	for i, v := range values {
		dv[i] = v
	}
	return dv
}

// baseColumns are the result columns present in all supported PG versions.
// The ten raw block count columns are selected individually; aggregation
// happens in Go, so the SQL query is free of arithmetic operators.
var baseColumns = []string{
	"user", "datname", "queryid",
	"calls_total", "seconds_total", "rows_total",
	"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
	"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
	"temp_blks_read", "temp_blks_written",
	"block_read_seconds_total", "block_write_seconds_total",
}

var tempIOTimingColumns = []string{"temp_block_read_seconds_total", "temp_block_write_seconds_total"}
var localIOTimingColumns = []string{"local_block_read_seconds_total", "local_block_write_seconds_total"}

// baseRow provides one row of test data aligned with baseColumns.
// Block counts:
//
//	shared: hit=3, read=5, dirtied=2, written=4
//	local:  hit=1, read=2, dirtied=1, written=1
//	temp:   read=3, written=2
//
// Timing (shared only, used for PG < 16):
//
//	block_read_seconds=0.1, block_write_seconds=0.2
//
// Aggregated count expectations:
//
//	blks_read_total    = 5+2+3 = 10
//	blks_written_total = 4+1+2 = 7
//	blks_hit_total     = 3+1   = 4   (no temp hit column)
//	blks_dirtied_total = 2+1   = 3   (no temp dirtied column)
var baseRow = []any{"postgres", "postgres", 1500, 5, 0.4, 100, 3, 5, 2, 4, 1, 2, 1, 1, 3, 2, 0.1, 0.2}

// expectedMetrics returns the 9 base metrics for the given aggregated timing values.
// Timing values depend on the PG version because we sum whichever pool timing
// columns the version exposes:
//
//	PG < 16:  blockRead=0.1, blockWrite=0.2  (shared only)
//	PG 16:    blockRead=0.4, blockWrite=0.6  (shared + temp: 0.1+0.3, 0.2+0.4)
//	PG 17+:   blockRead=0.45, blockWrite=0.66 (shared + temp + local)
func expectedMetrics(blockReadSeconds, blockWriteSeconds float64) []MetricResult {
	return []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},               // calls
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},              // seconds
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},              // rows
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 10},               // blks_read (5+2+3)
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 7},                // blks_written (4+1+2)
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 4},                // blks_hit (3+1)
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 3},                // blks_dirtied (2+1)
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: blockReadSeconds},  // block_read_seconds
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: blockWriteSeconds}, // block_write_seconds
	}
}

// baseExpected covers PG < 16: timing is shared blocks only.
var baseExpected = expectedMetrics(0.1, 0.2)

func TestPGStateStatementsCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	rows := sqlmock.NewRows(baseColumns).AddRow(toDriverValues(baseRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, ""))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range baseExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollectorWithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	stmtColumns := make([]string, 0, 1+len(baseColumns))
	stmtColumns = append(stmtColumns, "user", "datname", "queryid", "LEFT(pg_stat_statements.query, 100) as query")
	stmtColumns = append(stmtColumns, baseColumns[3:]...)

	stmtRow := make([]any, 0, 1+len(baseRow))
	stmtRow = append(stmtRow, "postgres", "postgres", 1500, "select 1 from foo")
	stmtRow = append(stmtRow, baseRow[3:]...)

	rows := sqlmock.NewRows(stmtColumns).AddRow(toDriverValues(stmtRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, fmt.Sprintf(pgStatStatementQuerySelect, 100)))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 100}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	queryExpected := make([]MetricResult, 0, len(baseExpected)+1)
	queryExpected = append(queryExpected, baseExpected...)
	queryExpected = append(queryExpected,
		MetricResult{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range queryExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollectorNull(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	nullRow := make([]any, len(baseColumns))
	rows := sqlmock.NewRows(baseColumns).AddRow(toDriverValues(nullRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsNewQuery, ""))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	nullExpected := make([]MetricResult, len(baseExpected))
	for i := range nullExpected {
		nullExpected[i] = MetricResult{
			labels:     labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"},
			metricType: dto.MetricType_COUNTER,
			value:      0,
		}
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range nullExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollectorNullWithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	stmtColumns := make([]string, 0, 1+len(baseColumns))
	stmtColumns = append(stmtColumns, "user", "datname", "queryid", "LEFT(pg_stat_statements.query, 200) as query")
	stmtColumns = append(stmtColumns, baseColumns[3:]...)
	nullRow := make([]any, len(stmtColumns))

	rows := sqlmock.NewRows(stmtColumns).AddRow(toDriverValues(nullRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsNewQuery, fmt.Sprintf(pgStatStatementQuerySelect, 200)))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 200}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	nullExpected := make([]MetricResult, len(baseExpected))
	for i := range nullExpected {
		nullExpected[i] = MetricResult{
			labels:     labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"},
			metricType: dto.MetricType_COUNTER,
			value:      0,
		}
	}
	nullExpected = append(nullExpected,
		MetricResult{labels: labelMap{"queryid": "unknown", "query": "unknown"}, metricType: dto.MetricType_COUNTER, value: 1},
	)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range nullExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollectorNewPG(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	rows := sqlmock.NewRows(baseColumns).AddRow(toDriverValues(baseRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsNewQuery, ""))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range baseExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollectorNewPGWithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	stmtColumns := make([]string, 0, 1+len(baseColumns))
	stmtColumns = append(stmtColumns, "user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query")
	stmtColumns = append(stmtColumns, baseColumns[3:]...)

	stmtRow := make([]any, 0, 1+len(baseRow))
	stmtRow = append(stmtRow, "postgres", "postgres", 1500, "select 1 from foo")
	stmtRow = append(stmtRow, baseRow[3:]...)

	rows := sqlmock.NewRows(stmtColumns).AddRow(toDriverValues(stmtRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsNewQuery, fmt.Sprintf(pgStatStatementQuerySelect, 300)))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	queryExpected := make([]MetricResult, 0, len(baseExpected)+1)
	queryExpected = append(queryExpected, baseExpected...)
	queryExpected = append(queryExpected,
		MetricResult{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range queryExpected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollector_PG16(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("16.0.0")}

	columns := make([]string, 0, len(baseColumns)+len(tempIOTimingColumns))
	columns = append(columns, baseColumns...)
	columns = append(columns, tempIOTimingColumns...)

	row := make([]any, 0, len(baseRow)+2)
	row = append(row, baseRow...)
	row = append(row, 0.3, 0.4)

	rows := sqlmock.NewRows(columns).AddRow(toDriverValues(row)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG16, ""))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	// Compute expected timing using runtime float64 arithmetic to match the
	// collector's sequential += additions (avoids constant-expression precision differences).
	var sharedRead, tempRead float64 = 0.1, 0.3
	var sharedWrite, tempWrite float64 = 0.2, 0.4
	expected := expectedMetrics(sharedRead+tempRead, sharedWrite+tempWrite)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollector_PG16_WithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("16.0.0")}

	stmtColumns := make([]string, 0, 1+len(baseColumns)+len(tempIOTimingColumns))
	stmtColumns = append(stmtColumns, "user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query")
	stmtColumns = append(stmtColumns, baseColumns[3:]...)
	stmtColumns = append(stmtColumns, tempIOTimingColumns...)

	stmtRow := make([]any, 0, 1+len(baseRow)+2)
	stmtRow = append(stmtRow, "postgres", "postgres", 1500, "select 1 from foo")
	stmtRow = append(stmtRow, baseRow[3:]...)
	stmtRow = append(stmtRow, 0.3, 0.4)

	rows := sqlmock.NewRows(stmtColumns).AddRow(toDriverValues(stmtRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG16, fmt.Sprintf(pgStatStatementQuerySelect, 300)))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	// Compute expected timing using runtime float64 arithmetic to match the
	// collector's sequential += additions (avoids constant-expression precision differences).
	var sharedRead, tempRead float64 = 0.1, 0.3
	var sharedWrite, tempWrite float64 = 0.2, 0.4
	expected := append(expectedMetrics(sharedRead+tempRead, sharedWrite+tempWrite),
		MetricResult{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollector_PG17(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	columns := make([]string, 0, len(baseColumns)+len(tempIOTimingColumns)+len(localIOTimingColumns))
	columns = append(columns, baseColumns...)
	columns = append(columns, tempIOTimingColumns...)
	columns = append(columns, localIOTimingColumns...)

	row := make([]any, 0, len(baseRow)+4)
	row = append(row, baseRow...)
	row = append(row, 0.3, 0.4, 0.05, 0.06)

	rows := sqlmock.NewRows(columns).AddRow(toDriverValues(row)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, ""))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	// Compute expected timing using runtime float64 arithmetic to match the
	// collector's sequential += additions (avoids constant-expression precision differences).
	var sharedRead, tempRead, localRead float64 = 0.1, 0.3, 0.05
	var sharedWrite, tempWrite, localWrite float64 = 0.2, 0.4, 0.06
	expected := expectedMetrics(sharedRead+tempRead+localRead, sharedWrite+tempWrite+localWrite)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStateStatementsCollector_PG17_WithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	stmtColumns := make([]string, 0, 1+len(baseColumns)+len(tempIOTimingColumns)+len(localIOTimingColumns))
	stmtColumns = append(stmtColumns, "user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query")
	stmtColumns = append(stmtColumns, baseColumns[3:]...)
	stmtColumns = append(stmtColumns, tempIOTimingColumns...)
	stmtColumns = append(stmtColumns, localIOTimingColumns...)

	stmtRow := make([]any, 0, 1+len(baseRow)+4)
	stmtRow = append(stmtRow, "postgres", "postgres", 1500, "select 1 from foo")
	stmtRow = append(stmtRow, baseRow[3:]...)
	stmtRow = append(stmtRow, 0.3, 0.4, 0.05, 0.06)

	rows := sqlmock.NewRows(stmtColumns).AddRow(toDriverValues(stmtRow)...)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, fmt.Sprintf(pgStatStatementQuerySelect, 300)))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	// Compute expected timing using runtime float64 arithmetic to match the
	// collector's sequential += additions (avoids constant-expression precision differences).
	var sharedRead, tempRead, localRead float64 = 0.1, 0.3, 0.05
	var sharedWrite, tempWrite, localWrite float64 = 0.2, 0.4, 0.06
	expected := append(expectedMetrics(sharedRead+tempRead+localRead, sharedWrite+tempWrite+localWrite),
		MetricResult{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	)

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
