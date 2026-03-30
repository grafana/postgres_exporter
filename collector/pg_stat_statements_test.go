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

// baseColumns are the columns present in all supported PG versions.
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

// baseRow is a full row of data for all versions, without temp/local timing.
var baseRow = []any{"postgres", "postgres", 1500, 5, 0.4, 100, 10, 20, 5, 15, 2, 3, 1, 4, 6, 7, 0.1, 0.2}

// baseExpected is the ordered list of metric results for a base row.
var baseExpected = []MetricResult{
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},    // calls
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},   // seconds
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},   // rows
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 10},    // shared_blks_hit
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 20},    // shared_blks_read
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},     // shared_blks_dirtied
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 15},    // shared_blks_written
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 2},     // local_blks_hit
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 3},     // local_blks_read
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 1},     // local_blks_dirtied
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 4},     // local_blks_written
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 6},     // temp_blks_read
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 7},     // temp_blks_written
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},   // block_read_seconds
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},   // block_write_seconds
}

var tempIOTimingExpected = []MetricResult{
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.3}, // temp_block_read_seconds
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4}, // temp_block_write_seconds
}

var localIOTimingExpected = []MetricResult{
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.05}, // local_block_read_seconds
	{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.06}, // local_block_write_seconds
}

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

	stmtColumns := append([]string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 100) as query"},
		baseColumns[3:]...)
	stmtRow := append([]any{"postgres", "postgres", 1500, "select 1 from foo"}, baseRow[3:]...)
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

	queryExpected := append(baseExpected,
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

	stmtColumns := append([]string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 200) as query"},
		baseColumns[3:]...)
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

	stmtColumns := append([]string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query"},
		baseColumns[3:]...)
	stmtRow := append([]any{"postgres", "postgres", 1500, "select 1 from foo"}, baseRow[3:]...)
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

	queryExpected := append(baseExpected,
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

	columns := append(baseColumns, tempIOTimingColumns...)
	row := append(baseRow, 0.3, 0.4)
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

	expected := append(baseExpected, tempIOTimingExpected...)

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

	stmtColumns := append(
		append([]string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query"}, baseColumns[3:]...),
		tempIOTimingColumns...,
	)
	stmtRow := append(append([]any{"postgres", "postgres", 1500, "select 1 from foo"}, baseRow[3:]...), 0.3, 0.4)
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

	expected := append(append(baseExpected, tempIOTimingExpected...),
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

	columns := append(append(baseColumns, tempIOTimingColumns...), localIOTimingColumns...)
	row := append(append(baseRow, 0.3, 0.4), 0.05, 0.06)
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

	expected := append(append(baseExpected, tempIOTimingExpected...), localIOTimingExpected...)

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

	stmtColumns := append(
		append(
			append([]string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query"}, baseColumns[3:]...),
			tempIOTimingColumns...,
		),
		localIOTimingColumns...,
	)
	stmtRow := append(append(append([]any{"postgres", "postgres", 1500, "select 1 from foo"}, baseRow[3:]...), 0.3, 0.4), 0.05, 0.06)
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

	expected := append(append(append(baseExpected, tempIOTimingExpected...), localIOTimingExpected...),
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
