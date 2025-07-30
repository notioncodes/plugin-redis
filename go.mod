module github.com/notioncodes/plugin-redis

go 1.24.2

require (
	github.com/mateothegreat/go-multilog v0.0.0-20250627190626-359729313052
	github.com/notioncodes/client v0.1.0
	github.com/notioncodes/plugin v0.0.0-20250729003550-6ddd14124e91
	github.com/notioncodes/test v0.0.0-20250730133732-14c47566e1d1
	github.com/notioncodes/types v0.1.0
	github.com/redis/go-redis/v9 v9.11.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/elastic/elastic-transport-go/v8 v8.6.0 // indirect
	github.com/elastic/go-elasticsearch/v8 v8.14.0 // indirect
	github.com/fatih/color v1.17.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mateothegreat/go-config v0.0.0-20250727162039-eb3615a83541 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/notioncodes/id v0.0.0-20250727200046-49fb310b2e53 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/spf13/pflag v1.0.7 // indirect
	go.opentelemetry.io/otel v1.29.0 // indirect
	go.opentelemetry.io/otel/metric v1.29.0 // indirect
	go.opentelemetry.io/otel/trace v1.29.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Replace with local modules for development
replace github.com/notioncodes/client => ../../client

replace github.com/notioncodes/types => ../../types
