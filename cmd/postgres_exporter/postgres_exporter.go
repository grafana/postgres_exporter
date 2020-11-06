package main

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/wrouesnel/postgres_exporter/exporter"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Branch is set during build to the git branch.
var Branch string

// BuildDate is set during build to the ISO-8601 date and time.
var BuildDate string

// Revision is set during build to the git commit revision.
var Revision string

// Version is set during build to the git describe version
// (semantic version)-(commitish) form.
var Version = "0.0.1-rev"

// VersionShort is set during build to the semantic version.
var VersionShort = "0.0.1"

var (
	listenAddress          = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9187").Envar("PG_EXPORTER_WEB_LISTEN_ADDRESS").String()
	metricPath             = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").Envar("PG_EXPORTER_WEB_TELEMETRY_PATH").String()
	disableDefaultMetrics  = kingpin.Flag("disable-default-metrics", "Do not include default metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_DEFAULT_METRICS").Bool()
	disableSettingsMetrics = kingpin.Flag("disable-settings-metrics", "Do not include pg_settings metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_SETTINGS_METRICS").Bool()
	autoDiscoverDatabases  = kingpin.Flag("auto-discover-databases", "Whether to discover the databases on a server dynamically.").Default("false").Envar("PG_EXPORTER_AUTO_DISCOVER_DATABASES").Bool()
	queriesPath            = kingpin.Flag("extend.query-path", "Path to custom queries to run.").Default("").Envar("PG_EXPORTER_EXTEND_QUERY_PATH").String()
	onlyDumpMaps           = kingpin.Flag("dumpmaps", "Do not run, simply dump the maps.").Bool()
	constantLabelsList     = kingpin.Flag("constantLabels", "A list of label=value separated by comma(,).").Default("").Envar("PG_EXPORTER_CONSTANT_LABELS").String()
	excludeDatabases       = kingpin.Flag("exclude-databases", "A list of databases to remove when autoDiscoverDatabases is enabled").Default("").Envar("PG_EXPORTER_EXCLUDE_DATABASES").String()
)

func main() {
	kingpin.Version(fmt.Sprintf("postgres_exporter %s (built with %s)\n", Version, runtime.Version()))
	log.AddFlags(kingpin.CommandLine)
	kingpin.Parse()

	// landingPage contains the HTML served at '/'.
	// TODO: Make this nicer and more informative.
	var landingPage = []byte(`<html>
	<head><title>Postgres exporter</title></head>
	<body>
	<h1>Postgres exporter</h1>
	<p><a href='` + *metricPath + `'>Metrics</a></p>
	</body>
	</html>
	`)

	if *onlyDumpMaps {
		exporter.DumpMaps()
		return
	}

	dsn := exporter.GetDataSources()
	if len(dsn) == 0 {
		log.Fatal("couldn't find environment variables describing the datasource to use")
	}

	e := exporter.NewExporter(dsn,
		exporter.DisableDefaultMetrics(*disableDefaultMetrics),
		exporter.DisableSettingsMetrics(*disableSettingsMetrics),
		exporter.AutoDiscoverDatabases(*autoDiscoverDatabases),
		exporter.WithUserQueriesPath(*queriesPath),
		exporter.WithConstantLabels(*constantLabelsList),
		exporter.ExcludeDatabases(*excludeDatabases),
	)
	defer func() {
		e.Close()
	}()

	// Setup build info metric.
	version.Branch = Branch
	version.BuildDate = BuildDate
	version.Revision = Revision
	version.Version = VersionShort
	prometheus.MustRegister(version.NewCollector("postgres_exporter"))

	prometheus.MustRegister(e)

	http.Handle(*metricPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8") // nolint: errcheck
		w.Write(landingPage)                                       // nolint: errcheck
	})

	log.Infof("Starting Server: %s", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
