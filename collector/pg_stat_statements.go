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
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const statStatementsSubsystem = "stat_statements"

var (
	includeQueryFlag    *bool = nil
	statementLengthFlag *uint = nil
)

func init() {
	// WARNING:
	//   Disabled by default because this set of metrics can be quite expensive on a busy server
	//   Every unique query will cause a new timeseries to be created
	registerCollector(statStatementsSubsystem, defaultDisabled, NewPGStatStatementsCollector)

	includeQueryFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".include_query"),
		"Enable selecting statement query together with queryId. (default: disabled)").
		Default(fmt.Sprintf("%v", defaultDisabled)).
		Bool()
	statementLengthFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".query_length"),
		"Maximum length of the statement text.").
		Default("120").
		Uint()
}

type PGStatStatementsCollector struct {
	log                   *slog.Logger
	includeQueryStatement bool
	statementLength       uint
}

func NewPGStatStatementsCollector(config collectorConfig) (Collector, error) {
	return &PGStatStatementsCollector{
		log:                   config.logger,
		includeQueryStatement: *includeQueryFlag,
		statementLength:       *statementLengthFlag,
	}, nil
}

var (
	statStatementsCallsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "calls_total"),
		"Number of times executed",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "seconds_total"),
		"Total time spent in the statement, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsRowsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "rows_total"),
		"Total number of rows retrieved or affected by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	// Shared block I/O timing. In PG 17+ this maps to shared_blk_read/write_time;
	// in earlier versions it maps to the legacy blk_read/write_time columns which
	// only tracked shared blocks.
	statStatementsBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_read_seconds_total"),
		"Total time the statement spent reading shared blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_write_seconds_total"),
		"Total time the statement spent writing shared blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	// Temp block I/O timing — available from PG 16.
	statStatementsTempBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "temp_block_read_seconds_total"),
		"Total time the statement spent reading temporary file blocks, in seconds (if track_io_timing is enabled, otherwise zero). Available from PostgreSQL 16.",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsTempBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "temp_block_write_seconds_total"),
		"Total time the statement spent writing temporary file blocks, in seconds (if track_io_timing is enabled, otherwise zero). Available from PostgreSQL 16.",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	// Local block I/O timing — available from PG 17.
	statStatementsLocalBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_block_read_seconds_total"),
		"Total time the statement spent reading local blocks, in seconds (if track_io_timing is enabled, otherwise zero). Available from PostgreSQL 17.",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsLocalBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_block_write_seconds_total"),
		"Total time the statement spent writing local blocks, in seconds (if track_io_timing is enabled, otherwise zero). Available from PostgreSQL 17.",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	// Aggregated block I/O counts. The raw per-pool columns are selected from
	// pg_stat_statements and aggregated in Go:
	//   blks_read_total    = shared + local + temp reads
	//   blks_written_total = shared + local + temp writes
	//   blks_hit_total     = shared + local hits  (temp has no hit column)
	//   blks_dirtied_total = shared + local dirtied (temp has no dirtied column)
	statStatementsBlksReadTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "blks_read_total"),
		"Total number of blocks read by the statement (shared + local + temp)",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlksWrittenTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "blks_written_total"),
		"Total number of blocks written by the statement (shared + local + temp)",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlksHitTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "blks_hit_total"),
		"Total number of block cache hits by the statement (shared + local)",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlksDirtiedTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "blks_dirtied_total"),
		"Total number of blocks dirtied by the statement (shared + local)",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsQuery = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "query_id"),
		"SQL Query to queryid mapping",
		[]string{"queryid", "query"},
		prometheus.Labels{},
	)
)

// pgStatStatementsBlockCounts selects the individual per-pool block I/O count
// columns. The dirtied columns were added in PG 9.2; since the collector
// already requires PG 9.4+ (for queryid), all ten columns are safe to select
// in every query variant. Aggregation is performed in Go to keep the SQL
// free of arithmetic expressions.
const pgStatStatementsBlockCounts = `
		pg_stat_statements.shared_blks_hit,
		pg_stat_statements.shared_blks_read,
		pg_stat_statements.shared_blks_dirtied,
		pg_stat_statements.shared_blks_written,
		pg_stat_statements.local_blks_hit,
		pg_stat_statements.local_blks_read,
		pg_stat_statements.local_blks_dirtied,
		pg_stat_statements.local_blks_written,
		pg_stat_statements.temp_blks_read,
		pg_stat_statements.temp_blks_written,`

const (
	pgStatStatementQuerySelect = `LEFT(pg_stat_statements.query, %d) as query,`

	// PG < 13: uses total_time; no temp/local block I/O timing columns.
	pgStatStatementsQuery = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,` +
		pgStatStatementsBlockCounts + `
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_time)
			FROM pg_stat_statements
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`

	// PG 13–15: uses total_exec_time; no temp/local block I/O timing columns.
	pgStatStatementsNewQuery = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,` +
		pgStatStatementsBlockCounts + `
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`

	// PG 16: adds temp_blk_read_time / temp_blk_write_time.
	pgStatStatementsQuery_PG16 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,` +
		pgStatStatementsBlockCounts + `
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total,
		pg_stat_statements.temp_blk_read_time / 1000.0 as temp_block_read_seconds_total,
		pg_stat_statements.temp_blk_write_time / 1000.0 as temp_block_write_seconds_total
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`

	// PG 17+: blk_read_time / blk_write_time were renamed to shared_blk_read_time /
	// shared_blk_write_time, and new local_blk_read_time / local_blk_write_time
	// columns were added.
	pgStatStatementsQuery_PG17 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,` +
		pgStatStatementsBlockCounts + `
		pg_stat_statements.shared_blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.shared_blk_write_time / 1000.0 as block_write_seconds_total,
		pg_stat_statements.temp_blk_read_time / 1000.0 as temp_block_read_seconds_total,
		pg_stat_statements.temp_blk_write_time / 1000.0 as temp_block_write_seconds_total,
		pg_stat_statements.local_blk_read_time / 1000.0 as local_block_read_seconds_total,
		pg_stat_statements.local_blk_write_time / 1000.0 as local_block_write_seconds_total
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`
)

func (c PGStatStatementsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	hasTempIOTiming := instance.version.GE(semver.MustParse("16.0.0"))
	hasLocalIOTiming := instance.version.GE(semver.MustParse("17.0.0"))

	var queryTemplate string
	switch {
	case hasLocalIOTiming:
		queryTemplate = pgStatStatementsQuery_PG17
	case hasTempIOTiming:
		queryTemplate = pgStatStatementsQuery_PG16
	case instance.version.GE(semver.MustParse("13.0.0")):
		queryTemplate = pgStatStatementsNewQuery
	default:
		queryTemplate = pgStatStatementsQuery
	}
	var querySelect = ""
	if c.includeQueryStatement {
		querySelect = fmt.Sprintf(pgStatStatementQuerySelect, c.statementLength)
	}
	query := fmt.Sprintf(queryTemplate, querySelect)

	db := instance.getDB()
	rows, err := db.QueryContext(ctx, query)

	var presentQueryIds = make(map[string]struct{})

	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var user, datname, queryid, statement sql.NullString
		var callsTotal, rowsTotal sql.NullInt64
		var sharedBlksHit, sharedBlksRead, sharedBlksDirtied, sharedBlksWritten sql.NullInt64
		var localBlksHit, localBlksRead, localBlksDirtied, localBlksWritten sql.NullInt64
		var tempBlksRead, tempBlksWritten sql.NullInt64
		var secondsTotal, blockReadSecondsTotal, blockWriteSecondsTotal sql.NullFloat64
		var tempBlockReadSecondsTotal, tempBlockWriteSecondsTotal sql.NullFloat64
		var localBlockReadSecondsTotal, localBlockWriteSecondsTotal sql.NullFloat64

		columns := []any{&user, &datname, &queryid}
		if c.includeQueryStatement {
			columns = append(columns, &statement)
		}
		columns = append(columns,
			&callsTotal, &secondsTotal, &rowsTotal,
			&sharedBlksHit, &sharedBlksRead, &sharedBlksDirtied, &sharedBlksWritten,
			&localBlksHit, &localBlksRead, &localBlksDirtied, &localBlksWritten,
			&tempBlksRead, &tempBlksWritten,
			&blockReadSecondsTotal, &blockWriteSecondsTotal,
		)
		if hasTempIOTiming {
			columns = append(columns, &tempBlockReadSecondsTotal, &tempBlockWriteSecondsTotal)
		}
		if hasLocalIOTiming {
			columns = append(columns, &localBlockReadSecondsTotal, &localBlockWriteSecondsTotal)
		}

		if err := rows.Scan(columns...); err != nil {
			return err
		}

		userLabel := "unknown"
		if user.Valid {
			userLabel = user.String
		}
		datnameLabel := "unknown"
		if datname.Valid {
			datnameLabel = datname.String
		}
		queryidLabel := "unknown"
		if queryid.Valid {
			queryidLabel = queryid.String
		}

		callsTotalMetric := 0.0
		if callsTotal.Valid {
			callsTotalMetric = float64(callsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsCallsTotal,
			prometheus.CounterValue,
			callsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		secondsTotalMetric := 0.0
		if secondsTotal.Valid {
			secondsTotalMetric = secondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsSecondsTotal,
			prometheus.CounterValue,
			secondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		rowsTotalMetric := 0.0
		if rowsTotal.Valid {
			rowsTotalMetric = float64(rowsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsRowsTotal,
			prometheus.CounterValue,
			rowsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		// Aggregate raw block counts across pool types.
		blksReadTotal := int64(0)
		if sharedBlksRead.Valid {
			blksReadTotal += sharedBlksRead.Int64
		}
		if localBlksRead.Valid {
			blksReadTotal += localBlksRead.Int64
		}
		if tempBlksRead.Valid {
			blksReadTotal += tempBlksRead.Int64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlksReadTotal,
			prometheus.CounterValue,
			float64(blksReadTotal),
			userLabel, datnameLabel, queryidLabel,
		)

		blksWrittenTotal := int64(0)
		if sharedBlksWritten.Valid {
			blksWrittenTotal += sharedBlksWritten.Int64
		}
		if localBlksWritten.Valid {
			blksWrittenTotal += localBlksWritten.Int64
		}
		if tempBlksWritten.Valid {
			blksWrittenTotal += tempBlksWritten.Int64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlksWrittenTotal,
			prometheus.CounterValue,
			float64(blksWrittenTotal),
			userLabel, datnameLabel, queryidLabel,
		)

		// temp blocks have no hit or dirtied columns in pg_stat_statements.
		blksHitTotal := int64(0)
		if sharedBlksHit.Valid {
			blksHitTotal += sharedBlksHit.Int64
		}
		if localBlksHit.Valid {
			blksHitTotal += localBlksHit.Int64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlksHitTotal,
			prometheus.CounterValue,
			float64(blksHitTotal),
			userLabel, datnameLabel, queryidLabel,
		)

		blksDirtiedTotal := int64(0)
		if sharedBlksDirtied.Valid {
			blksDirtiedTotal += sharedBlksDirtied.Int64
		}
		if localBlksDirtied.Valid {
			blksDirtiedTotal += localBlksDirtied.Int64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlksDirtiedTotal,
			prometheus.CounterValue,
			float64(blksDirtiedTotal),
			userLabel, datnameLabel, queryidLabel,
		)

		blockReadSecondsTotalMetric := 0.0
		if blockReadSecondsTotal.Valid {
			blockReadSecondsTotalMetric = blockReadSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockReadSecondsTotal,
			prometheus.CounterValue,
			blockReadSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		blockWriteSecondsTotalMetric := 0.0
		if blockWriteSecondsTotal.Valid {
			blockWriteSecondsTotalMetric = blockWriteSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockWriteSecondsTotal,
			prometheus.CounterValue,
			blockWriteSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		if hasTempIOTiming {
			tempBlockReadSecondsTotalMetric := 0.0
			if tempBlockReadSecondsTotal.Valid {
				tempBlockReadSecondsTotalMetric = tempBlockReadSecondsTotal.Float64
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsTempBlockReadSecondsTotal,
				prometheus.CounterValue,
				tempBlockReadSecondsTotalMetric,
				userLabel, datnameLabel, queryidLabel,
			)

			tempBlockWriteSecondsTotalMetric := 0.0
			if tempBlockWriteSecondsTotal.Valid {
				tempBlockWriteSecondsTotalMetric = tempBlockWriteSecondsTotal.Float64
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsTempBlockWriteSecondsTotal,
				prometheus.CounterValue,
				tempBlockWriteSecondsTotalMetric,
				userLabel, datnameLabel, queryidLabel,
			)
		}

		if hasLocalIOTiming {
			localBlockReadSecondsTotalMetric := 0.0
			if localBlockReadSecondsTotal.Valid {
				localBlockReadSecondsTotalMetric = localBlockReadSecondsTotal.Float64
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlockReadSecondsTotal,
				prometheus.CounterValue,
				localBlockReadSecondsTotalMetric,
				userLabel, datnameLabel, queryidLabel,
			)

			localBlockWriteSecondsTotalMetric := 0.0
			if localBlockWriteSecondsTotal.Valid {
				localBlockWriteSecondsTotalMetric = localBlockWriteSecondsTotal.Float64
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlockWriteSecondsTotal,
				prometheus.CounterValue,
				localBlockWriteSecondsTotalMetric,
				userLabel, datnameLabel, queryidLabel,
			)
		}

		if c.includeQueryStatement {
			_, ok := presentQueryIds[queryidLabel]
			if !ok {
				presentQueryIds[queryidLabel] = struct{}{}

				queryLabel := "unknown"
				if statement.Valid {
					queryLabel = statement.String
				}

				ch <- prometheus.MustNewConstMetric(
					statStatementsQuery,
					prometheus.CounterValue,
					1,
					queryidLabel, queryLabel,
				)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
