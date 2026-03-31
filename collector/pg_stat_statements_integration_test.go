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

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

// TestPGStatStatementsIntegration connects to a live PostgreSQL instance,
// installs pg_stat_statements, executes a workload, and verifies that the
// collector emits all expected metrics without errors.
//
// Prerequisites:
//   - PostgreSQL must have been started with
//     shared_preload_libraries = 'pg_stat_statements'
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

	// Use the same queryVersion helper as the collector itself.
	pgVersion, err := queryVersion(db)
	if err != nil {
		t.Fatalf("query version: %v", err)
	}
	t.Logf("Detected PG version: %s", pgVersion)

	// Enable pg_stat_statements (requires shared_preload_libraries to include it).
	if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		if strings.Contains(err.Error(), "shared_preload_libraries") {
			t.Skipf("pg_stat_statements is not loaded via shared_preload_libraries; "+
				"add pg_stat_statements to shared_preload_libraries and restart PostgreSQL. "+
				"Original error: %v", err)
		}
		t.Fatalf("create extension pg_stat_statements: %v", err)
	}

	// Reset stats so our subsequent queries are visible.
	if _, err := db.Exec("SELECT pg_stat_statements_reset()"); err != nil {
		t.Logf("pg_stat_statements_reset() failed (may need superuser): %v", err)
	}

	// Generate some query activity so there is at least one row in pg_stat_statements.
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(fmt.Sprintf("SELECT %d + %d", i, i+1)); err != nil {
			t.Fatalf("workload query %d: %v", i, err)
		}
	}

	// Run the collector, draining metrics in a goroutine to avoid blocking
	// Update() when the channel buffer fills up (up to 100 rows × 9 metrics).
	inst := &instance{db: db, version: pgVersion}
	c := PGStatStatementsCollector{}

	ch := make(chan prometheus.Metric, 1000)
	if err := c.Update(context.Background(), inst, ch); err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	close(ch)

	// Tally which metric names were emitted using the fqName field from Desc.String().
	seen := make(map[string]int)
	for m := range ch {
		desc := m.Desc().String()
		const fqPrefix = `fqName: "`
		start := strings.Index(desc, fqPrefix)
		if start == -1 {
			continue
		}
		start += len(fqPrefix)
		end := strings.Index(desc[start:], `"`)
		if end == -1 {
			continue
		}
		seen[desc[start:start+end]]++
	}

	t.Logf("Emitted metric names: %v", seen)

	// These metrics must always be present regardless of PostgreSQL version.
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

	t.Logf("PG %d: all required metrics emitted (%d total metric series)", pgVersion.Major, len(seen))
}
