// Copyright 2025 The Prometheus Authors
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

//go:build integration
// +build integration

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/blang/semver/v4"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

// TestPGStatStatementsIntegration connects to a live PostgreSQL instance,
// installs pg_stat_statements, executes a workload, and verifies that the
// collector emits all expected metrics without errors.
//
// Run with:
//
//	DATA_SOURCE_NAME="postgresql://postgres:test@localhost:5432/circle_test?sslmode=disable" \
//	  go test -v -tags integration ./collector/ -run TestPGStatStatementsIntegration
func TestPGStatStatementsIntegration(t *testing.T) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	if dsn == "" {
		t.Skip("DATA_SOURCE_NAME not set; skipping integration test")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping db: %v", err)
	}

	// Determine PostgreSQL version.
	var versionStr string
	if err := db.QueryRow("SELECT version()").Scan(&versionStr); err != nil {
		t.Fatalf("query version: %v", err)
	}
	t.Logf("PostgreSQL version string: %s", versionStr)

	var pgMajor, pgMinor int
	if _, err := fmt.Sscanf(strings.TrimPrefix(strings.Split(versionStr, " ")[1], ""), "%d.%d", &pgMajor, &pgMinor); err != nil {
		// Single-digit minor (e.g. "17" not "17.0")
		fmt.Sscanf(strings.Split(versionStr, " ")[1], "%d", &pgMajor)
	}
	semverStr := fmt.Sprintf("%d.%d.0", pgMajor, pgMinor)
	pgVersion, err := semver.ParseTolerant(semverStr)
	if err != nil {
		t.Fatalf("parse semver %q: %v", semverStr, err)
	}
	t.Logf("Detected PG version: %s", pgVersion)

	// Enable pg_stat_statements (requires the extension to be available).
	if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		t.Fatalf("create extension pg_stat_statements: %v", err)
	}

	// Reset stats so our subsequent queries are visible.
	if _, err := db.Exec("SELECT pg_stat_statements_reset()"); err != nil {
		t.Logf("pg_stat_statements_reset() failed (may need superuser): %v", err)
	}

	// Generate some query activity.
	for i := 0; i < 5; i++ {
		if _, err := db.Exec("SELECT $1::int + $2::int", i, i+1); err != nil {
			t.Fatalf("workload query %d: %v", i, err)
		}
	}

	// Run the collector.
	inst := &instance{db: db, version: pgVersion}
	c := PGStatStatementsCollector{}

	ch := make(chan prometheus.Metric, 200)
	if err := c.Update(context.Background(), inst, ch); err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	close(ch)

	// Tally which metric names were emitted.
	seen := make(map[string]int)
	for m := range ch {
		desc := m.Desc().String()
		// Extract metric name from the fqName field in the desc string.
		// Desc format: Desc{fqName: "pg_stat_statements_calls_total", ...}
		start := strings.Index(desc, `"`) + 1
		end := strings.Index(desc[start:], `"`) + start
		if start > 0 && end > start {
			seen[desc[start:end]]++
		}
	}

	t.Logf("Emitted metric names: %v", seen)

	// These metrics must always be present.
	required := []string{
		"pg_stat_statements_calls_total",
		"pg_stat_statements_seconds_total",
		"pg_stat_statements_rows_total",
		"pg_stat_statements_blks_read_total",
		"pg_stat_statements_blks_written_total",
		"pg_stat_statements_blks_hit_total",
		"pg_stat_statements_blks_dirtied_total",
		"pg_stat_statements_block_read_seconds_total",
		"pg_stat_statements_block_write_seconds_total",
	}
	for _, name := range required {
		if seen[name] == 0 {
			t.Errorf("expected metric %q was not emitted", name)
		}
	}

	t.Logf("PG %d: all required metrics emitted (%d total metric series)", pgMajor, len(seen))
}
