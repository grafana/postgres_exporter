// Copyright 2021 The Prometheus Authors
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

package postgres_exporter

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus-community/postgres_exporter/collector"
	"github.com/prometheus-community/postgres_exporter/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/prometheus/exporter-toolkit/web/kingpinflag"
)

var (
	c = config.Handler{
		Config: &config.Config{},
	}

	configFile             = kingpin.Flag("config.file", "Postgres exporter configuration file.").Default("postgres_exporter.yml").String()
	webConfig              = kingpinflag.AddFlags(kingpin.CommandLine, ":9187")
	metricsPath            = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").Envar("PG_EXPORTER_WEB_TELEMETRY_PATH").String()
	disableDefaultMetrics  = kingpin.Flag("disable-default-metrics", "Do not include default metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_DEFAULT_METRICS").Bool()
	disableSettingsMetrics = kingpin.Flag("disable-settings-metrics", "Do not include pg_settings metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_SETTINGS_METRICS").Bool()
	autoDiscoverDatabases  = kingpin.Flag("auto-discover-databases", "Whether to discover the databases on a server dynamically. (DEPRECATED)").Default("false").Envar("PG_EXPORTER_AUTO_DISCOVER_DATABASES").Bool()
	queriesPath            = kingpin.Flag("extend.query-path", "Path to custom queries to run. (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXTEND_QUERY_PATH").String()
	onlyDumpMaps           = kingpin.Flag("dumpmaps", "Do not run, simply dump the maps.").Bool()
	constantLabelsList     = kingpin.Flag("constantLabels", "A list of label=value separated by comma(,). (DEPRECATED)").Default("").Envar("PG_EXPORTER_CONSTANT_LABELS").String()
	excludeDatabases       = kingpin.Flag("exclude-databases", "A list of databases to remove when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXCLUDE_DATABASES").String()
	includeDatabases       = kingpin.Flag("include-databases", "A list of databases to include when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_INCLUDE_DATABASES").String()

	// rootFallbackLogger is used when we cannot use the regular logger in this fork. Under normal operations, there shouldn't be anything logged using this.
	rootFallbackLogger = log.NewLogfmtLogger(os.Stderr)
)

// Metric name parts.
const (
	// Namespace for all metrics.
	namespace = "pg"
	// Subsystems.
	exporter = "exporter"
	// The name of the exporter.
	exporterName = "postgres_exporter"
	// Metric label used for static string data thats handy to send to Prometheus
	// e.g. version
	staticLabelName = "static"
	// Metric label used for server identification.
	serverLabelName = "server"
)

func main() {
	kingpin.Version(version.Print(exporterName))
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	if *onlyDumpMaps {
		dumpMaps()
		return
	}

	if err := c.ReloadConfig(*configFile, rootFallbackLogger); err != nil {
		// This is not fatal, but it means that auth must be provided for every dsn.
		level.Warn(rootFallbackLogger).Log("msg", "Error loading config", "err", err)
	}

	dsns, err := getDataSources()
	if err != nil {
		level.Error(rootFallbackLogger).Log("msg", "Failed reading data sources", "err", err.Error())
		os.Exit(1)
	}

	excludedDatabases := strings.Split(*excludeDatabases, ",")
	rootFallbackLogger.Log("msg", "Excluded databases", "databases", fmt.Sprintf("%v", excludedDatabases))

	if *queriesPath != "" {
		level.Warn(rootFallbackLogger).Log("msg", "The extended queries.yaml config is DEPRECATED", "file", *queriesPath)
	}

	if *autoDiscoverDatabases || *excludeDatabases != "" || *includeDatabases != "" {
		level.Warn(rootFallbackLogger).Log("msg", "Scraping additional databases via auto discovery is DEPRECATED")
	}

	if *constantLabelsList != "" {
		level.Warn(rootFallbackLogger).Log("msg", "Constant labels on all metrics is DEPRECATED")
	}

	opts := []ExporterOpt{
		DisableDefaultMetrics(*disableDefaultMetrics),
		DisableSettingsMetrics(*disableSettingsMetrics),
		AutoDiscoverDatabases(*autoDiscoverDatabases),
		WithUserQueriesPath(*queriesPath),
		WithConstantLabels(*constantLabelsList),
		ExcludeDatabases(excludedDatabases),
		IncludeDatabases(*includeDatabases),
	}

	exporter := NewExporter(dsns, opts...)
	defer func() {
		exporter.servers.Close()
	}()

	prometheus.MustRegister(version.NewCollector(exporterName))

	prometheus.MustRegister(exporter)

	// TODO(@sysadmind): Remove this with multi-target support. We are removing multiple DSN support
	dsn := ""
	if len(dsns) > 0 {
		dsn = dsns[0]
	}

	pe, err := collector.NewPostgresCollector(
		rootFallbackLogger,
		excludedDatabases,
		dsn,
		[]string{},
	)
	if err != nil {
		level.Warn(rootFallbackLogger).Log("msg", "Failed to create PostgresCollector", "err", err.Error())
	} else {
		prometheus.MustRegister(pe)
	}

	http.Handle(*metricsPath, promhttp.Handler())

	if *metricsPath != "/" && *metricsPath != "" {
		landingConfig := web.LandingConfig{
			Name:        "Postgres Exporter",
			Description: "Prometheus PostgreSQL server Exporter",
			Version:     version.Info(),
			Links: []web.LandingLinks{
				{
					Address: *metricsPath,
					Text:    "Metrics",
				},
			},
		}
		landingPage, err := web.NewLandingPage(landingConfig)
		if err != nil {
			level.Error(rootFallbackLogger).Log("err", err)
			os.Exit(1)
		}
		http.Handle("/", landingPage)
	}

	http.HandleFunc("/probe", handleProbe(rootFallbackLogger, excludedDatabases))

	srv := &http.Server{}
	if err := web.ListenAndServe(srv, webConfig, rootFallbackLogger); err != nil {
		level.Error(rootFallbackLogger).Log("msg", "Error running HTTP server", "err", err)
		os.Exit(1)
	}
}
