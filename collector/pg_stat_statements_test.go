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
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestPGStatStatementsCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, "", "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

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

func TestPGStatStatementsCollectorWithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 100) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, fmt.Sprintf(pgStatStatementQuerySelect, 100), "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 100}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollectorWithStatementAndLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 100) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, fmt.Sprintf(pgStatStatementQuerySelect, 100), "", "", "10"))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 100, statementLimit: 10}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollectorNull(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow(nil, nil, nil, nil, nil, nil, nil, nil)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, "", false, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
	}

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

func TestPGStatStatementsCollectorNullWithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 200) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, fmt.Sprintf(pgStatStatementQuerySelect, 200), true, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 200}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"queryid": "unknown", "query": "unknown"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollectorNullWithStatementAndLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 200) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, fmt.Sprintf(pgStatStatementQuerySelect, 200), true, "", "", "10"))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 200, statementLimit: 10}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"user": "unknown", "datname": "unknown", "queryid": "unknown"}, metricType: dto.MetricType_COUNTER, value: 0},
		{labels: labelMap{"queryid": "unknown", "query": "unknown"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollector_PG13(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, "", false, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

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

func TestPGStatStatementsCollector_PG13_WithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, fmt.Sprintf(pgStatStatementQuerySelect, 300), true, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollector_PG13_WithStatementAndLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.3.7")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, fmt.Sprintf(pgStatStatementQuerySelect, 300), true, "", "", "10"))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300, statementLimit: 10}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollector_PG17(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, "", false, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

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

func TestPGStatStatementsCollector_PG17_WithStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, fmt.Sprintf(pgStatStatementQuerySelect, 300), true, "", "", defaultStatementLimit))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollector_PG17_WithStatementAndLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	columns := []string{"user", "datname", "queryid", "LEFT(pg_stat_statements.query, 300) as query", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, "select 1 from foo", 5, 0.4, 100, 0.1, 0.2)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, fmt.Sprintf(pgStatStatementQuerySelect, 300), true, "", "", "10"))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{includeQueryStatement: true, statementLength: 300, statementLimit: 10}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
		{labels: labelMap{"queryid": "1500", "query": "select 1 from foo"}, metricType: dto.MetricType_COUNTER, value: 1},
	}

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

func TestPGStatStatementsCollectorWithExcludedDatabases(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	excludedDatabases := []string{"rdsadmin", "cloudsqladmin"}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)

	// Expected query should include database exclusion filters in both the threshold subquery and outer query
	expectedThresholdFilter := "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_database.datname != ALL($1)"
	expectedFilter := "AND pg_database.datname != ALL($1)"
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, "", expectedThresholdFilter, expectedFilter, defaultStatementLimit))).
		WithArgs(pq.Array(excludedDatabases)).
		WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{excludedDatabases: excludedDatabases}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

	convey.Convey("Metrics comparison with excluded databases", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStatStatementsCollectorWithExcludedDatabasesSQLEscaping(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.0.0")}

	// exclude a database name with a single quote (SQL injection test)
	excludedDatabases := []string{"test'db"}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)

	expectedThresholdFilter := "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_database.datname != ALL($1)"
	expectedFilter := "AND pg_database.datname != ALL($1)"
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, "", false, expectedThresholdFilter, expectedFilter, defaultStatementLimit))).
		WithArgs(pq.Array(excludedDatabases)).
		WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{excludedDatabases: excludedDatabases}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

	convey.Convey("Metrics comparison with SQL escaping", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStatStatementsCollectorWithExcludedUsers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	excludedUsers := []string{"monitoring", "readonly"}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)

	expectedThresholdFilter := "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_get_userbyid(userid) != ALL($1)"
	expectedFilter := "AND pg_get_userbyid(userid) != ALL($1)"
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery, "", expectedThresholdFilter, expectedFilter, defaultStatementLimit))).
		WithArgs(pq.Array(excludedUsers)).
		WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{excludedUsers: excludedUsers}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

	convey.Convey("Metrics comparison with excluded users", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStatStatementsCollectorWithExcludedUsersSQLEscaping(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("13.0.0")}

	// exclude a user name with a single quote (SQL injection test)
	excludedUsers := []string{"test'user"}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)

	expectedThresholdFilter := "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_get_userbyid(userid) != ALL($1)"
	expectedFilter := "AND pg_get_userbyid(userid) != ALL($1)"
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG13, "", false, expectedThresholdFilter, expectedFilter, defaultStatementLimit))).
		WithArgs(pq.Array(excludedUsers)).
		WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{excludedUsers: excludedUsers}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

	convey.Convey("Metrics comparison with SQL escaping for users", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStatStatementsCollectorWithExcludedDatabasesAndUsers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("17.0.0")}

	// exclude both databases and users
	excludedDatabases := []string{"rdsadmin", "cloudsqladmin"}
	excludedUsers := []string{"monitoring", "readonly"}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "postgres", 1500, 5, 0.4, 100, 0.1, 0.2)

	expectedThresholdFilter := "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_database.datname != ALL($1) AND pg_get_userbyid(userid) != ALL($2)"
	expectedFilter := "AND pg_database.datname != ALL($1) AND pg_get_userbyid(userid) != ALL($2)"
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(pgStatStatementsQuery_PG17, "", false, expectedThresholdFilter, expectedFilter, defaultStatementLimit))).
		WithArgs(pq.Array(excludedDatabases), pq.Array(excludedUsers)).
		WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatStatementsCollector{excludedDatabases: excludedDatabases, excludedUsers: excludedUsers}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
		{labels: labelMap{"user": "postgres", "datname": "postgres", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
	}

	convey.Convey("Metrics comparison with excluded databases and users", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestPGStatStatementsCollectorWithIncludeFilters(t *testing.T) {
	tests := []struct {
		name                string
		version             string
		includedDatabases   []string
		excludedDatabases   []string
		includedUsers       []string
		excludedUsers       []string
		wantThresholdFilter string
		wantFilter          string
		wantArgs            []driver.Value
	}{
		{
			name:                "include databases only",
			version:             "12.0.0",
			includedDatabases:   []string{"app", "orders"},
			wantThresholdFilter: "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_database.datname = ANY($1)",
			wantFilter:          "AND pg_database.datname = ANY($1)",
			wantArgs:            []driver.Value{pq.Array([]string{"app", "orders"})},
		},
		{
			name:                "include users only",
			version:             "13.0.0",
			includedUsers:       []string{"app_user", "test'user"},
			wantThresholdFilter: "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_get_userbyid(userid) = ANY($1)",
			wantFilter:          "AND pg_get_userbyid(userid) = ANY($1)",
			wantArgs:            []driver.Value{pq.Array([]string{"app_user", "test'user"})},
		},
		{
			name:                "include and exclude databases and users",
			version:             "17.0.0",
			includedDatabases:   []string{"app"},
			excludedDatabases:   []string{"rdsadmin"},
			includedUsers:       []string{"app_user"},
			excludedUsers:       []string{"monitoring"},
			wantThresholdFilter: "JOIN pg_database ON pg_database.oid = pg_stat_statements.dbid WHERE pg_database.datname = ANY($1) AND pg_database.datname != ALL($2) AND pg_get_userbyid(userid) = ANY($3) AND pg_get_userbyid(userid) != ALL($4)",
			wantFilter:          "AND pg_database.datname = ANY($1) AND pg_database.datname != ALL($2) AND pg_get_userbyid(userid) = ANY($3) AND pg_get_userbyid(userid) != ALL($4)",
			wantArgs: []driver.Value{
				pq.Array([]string{"app"}),
				pq.Array([]string{"rdsadmin"}),
				pq.Array([]string{"app_user"}),
				pq.Array([]string{"monitoring"}),
			},
		},
	}

	columns := []string{"user", "datname", "queryid", "calls_total", "seconds_total", "rows_total", "block_read_seconds_total", "block_write_seconds_total"}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("Error opening a stub db connection: %s", err)
			}
			defer db.Close()

			ver := semver.MustParse(tc.version)
			inst := &instance{db: db, version: ver}

			rows := sqlmock.NewRows(columns).
				AddRow("app_user", "app", 1500, 5, 0.4, 100, 0.1, 0.2)

			var expectedQuery string
			switch {
			case ver.GE(semver.MustParse("17.0.0")):
				expectedQuery = fmt.Sprintf(pgStatStatementsQuery_PG17, "", false, tc.wantThresholdFilter, tc.wantFilter, defaultStatementLimit)
			case ver.GE(semver.MustParse("13.0.0")):
				expectedQuery = fmt.Sprintf(pgStatStatementsQuery_PG13, "", false, tc.wantThresholdFilter, tc.wantFilter, defaultStatementLimit)
			default:
				expectedQuery = fmt.Sprintf(pgStatStatementsQuery, "", tc.wantThresholdFilter, tc.wantFilter, defaultStatementLimit)
			}

			mock.ExpectQuery(sanitizeQuery(expectedQuery)).WithArgs(tc.wantArgs...).WillReturnRows(rows)

			ch := make(chan prometheus.Metric)
			go func() {
				defer close(ch)
				c := PGStatStatementsCollector{
					includedDatabases: tc.includedDatabases,
					excludedDatabases: tc.excludedDatabases,
					includedUsers:     tc.includedUsers,
					excludedUsers:     tc.excludedUsers,
				}
				if err := c.Update(context.Background(), inst, ch); err != nil {
					t.Errorf("Error calling PGStatStatementsCollector.Update: %s", err)
				}
			}()

			expected := []MetricResult{
				{labels: labelMap{"user": "app_user", "datname": "app", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 5},
				{labels: labelMap{"user": "app_user", "datname": "app", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.4},
				{labels: labelMap{"user": "app_user", "datname": "app", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 100},
				{labels: labelMap{"user": "app_user", "datname": "app", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.1},
				{labels: labelMap{"user": "app_user", "datname": "app", "queryid": "1500"}, metricType: dto.MetricType_COUNTER, value: 0.2},
			}

			convey.Convey("Metrics comparison with include filters", t, func() {
				for _, expect := range expected {
					m := readMetric(<-ch)
					convey.So(expect, convey.ShouldResemble, m)
				}
			})
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		})
	}
}
