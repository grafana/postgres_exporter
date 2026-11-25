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
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestPGLocksCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	rows := sqlmock.NewRows([]string{"datname", "mode", "count"}).
		AddRow("test", "exclusivelock", 42)

	mock.ExpectQuery(sanitizeQuery(pgLocksQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGLocksCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGLocksCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"datname": "test", "mode": "exclusivelock"}, value: 42, metricType: dto.MetricType_GAUGE},
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

func TestPGLocksCollectorWithFilters(t *testing.T) {
	tests := []struct {
		name              string
		includedDatabases []string
		excludedDatabases []string
		wantWhere         string
		wantArgs          []driver.Value
	}{
		{
			name:              "include databases",
			includedDatabases: []string{"app", "orders"},
			wantWhere:         "WHERE pg_database.datname = ANY($1)\n\t\t",
			wantArgs:          []driver.Value{pq.Array([]string{"app", "orders"})},
		},
		{
			name:              "exclude databases",
			excludedDatabases: []string{"rdsadmin", "cloudsqladmin"},
			wantWhere:         "WHERE pg_database.datname != ALL($1)\n\t\t",
			wantArgs:          []driver.Value{pq.Array([]string{"rdsadmin", "cloudsqladmin"})},
		},
		{
			name:              "include and exclude databases",
			includedDatabases: []string{"app"},
			excludedDatabases: []string{"rdsadmin"},
			wantWhere:         "WHERE pg_database.datname = ANY($1) AND pg_database.datname != ALL($2)\n\t\t",
			wantArgs:          []driver.Value{pq.Array([]string{"app"}), pq.Array([]string{"rdsadmin"})},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("Error opening a stub db connection: %s", err)
			}
			defer db.Close()

			inst := &instance{db: db}
			rows := sqlmock.NewRows([]string{"datname", "mode", "count"}).
				AddRow("app", "accesssharelock", 7)

			expectedQuery := fmt.Sprintf(pgLocksQueryTemplate, tc.wantWhere)
			mock.ExpectQuery(sanitizeQuery(expectedQuery)).WithArgs(tc.wantArgs...).WillReturnRows(rows)

			ch := make(chan prometheus.Metric)
			go func() {
				defer close(ch)
				c := PGLocksCollector{
					includedDatabases: tc.includedDatabases,
					excludedDatabases: tc.excludedDatabases,
				}
				if err := c.Update(context.Background(), inst, ch); err != nil {
					t.Errorf("Error calling PGLocksCollector.Update: %s", err)
				}
			}()

			expected := MetricResult{
				labels:     labelMap{"datname": "app", "mode": "accesssharelock"},
				value:      7,
				metricType: dto.MetricType_GAUGE,
			}
			convey.Convey("Metrics comparison", t, func() {
				m := readMetric(<-ch)
				convey.So(expected, convey.ShouldResemble, m)
			})
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		})
	}
}
