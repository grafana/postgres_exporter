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
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const locksSubsystem = "locks"

var (
	locksIncludedDatabasesFlag *string = nil
	locksExcludedDatabasesFlag *string = nil
)

func init() {
	registerCollector(locksSubsystem, defaultEnabled, NewPGLocksCollector)

	locksIncludedDatabasesFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, locksSubsystem, ".include_databases"),
		"Comma-separated list of database names to include. (default: all)").
		Default("").
		String()
	locksExcludedDatabasesFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, locksSubsystem, ".exclude_databases"),
		"Comma-separated list of database names to exclude. (default: none)").
		Default("").
		String()
}

type PGLocksCollector struct {
	log               *slog.Logger
	includedDatabases []string
	excludedDatabases []string
}

func NewPGLocksCollector(config collectorConfig) (Collector, error) {
	// When config is provided, use it directly instead of global kingpin flags
	cfg := config.locksConfig
	if cfg == nil {
		// Fall back to kingpin CLI flags for standalone postgres_exporter binary usage.
		cfg = &LocksConfig{
			IncludeDatabases: splitList(*locksIncludedDatabasesFlag),
			ExcludeDatabases: splitList(*locksExcludedDatabasesFlag),
		}
		if err := cfg.validate(); err != nil {
			return nil, err
		}
	}

	return &PGLocksCollector{
		log:               config.logger,
		includedDatabases: cfg.IncludeDatabases,
		excludedDatabases: cfg.ExcludeDatabases,
	}, nil
}

var (
	pgLocksDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			locksSubsystem,
			"count",
		),
		"Number of locks",
		[]string{"datname", "mode"}, nil,
	)

	pgLocksQueryTemplate = `
		SELECT 
		  pg_database.datname as datname,
		  tmp.mode as mode,
		  COALESCE(count, 0) as count 
		FROM 
		  (
		    VALUES 
		      ('accesssharelock'), 
		      ('rowsharelock'), 
		      ('rowexclusivelock'), 
		      ('shareupdateexclusivelock'), 
		      ('sharelock'), 
		      ('sharerowexclusivelock'), 
		      ('exclusivelock'), 
		      ('accessexclusivelock'), 
		      ('sireadlock')
		  ) AS tmp(mode)
		  CROSS JOIN pg_database 
		  LEFT JOIN (
		    SELECT 
		      database, 
		      lower(mode) AS mode, 
		      count(*) AS count 
		    FROM 
		      pg_locks 
		    WHERE 
		      database IS NOT NULL 
		    GROUP BY 
		      database, 
		      lower(mode)
		  ) AS tmp2 ON tmp.mode = tmp2.mode 
		  and pg_database.oid = tmp2.database 
		%sORDER BY 
		  1
	`

	pgLocksQuery = fmt.Sprintf(pgLocksQueryTemplate, "")
)

func locksQueryWithFilters(includedDatabases, excludedDatabases []string) (string, []any) {
	var filter sqlFilterBuilder
	filter.addIncludeExclude("pg_database.datname", includedDatabases, excludedDatabases)
	whereClause, args := filter.whereClause()
	if whereClause == "" {
		return pgLocksQuery, nil
	}
	return fmt.Sprintf(pgLocksQueryTemplate, whereClause+"\n\t\t"), args
}

// Update implements Collector and exposes database locks.
// It is called by the Prometheus registry when collecting metrics.
func (c PGLocksCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	query, args := locksQueryWithFilters(c.includedDatabases, c.excludedDatabases)
	// Query the list of databases
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var datname, mode sql.NullString
	var count sql.NullInt64

	for rows.Next() {
		if err := rows.Scan(&datname, &mode, &count); err != nil {
			return err
		}

		if !datname.Valid || !mode.Valid {
			continue
		}

		countMetric := 0.0
		if count.Valid {
			countMetric = float64(count.Int64)
		}

		ch <- prometheus.MustNewConstMetric(
			pgLocksDesc,
			prometheus.GaugeValue, countMetric,
			datname.String, mode.String,
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
