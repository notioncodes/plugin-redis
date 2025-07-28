module github.com/notioncodes/plugin-redis

go 1.24.2

require (
	github.com/notioncodes/client v0.1.0
	github.com/notioncodes/types v0.1.0
	github.com/redis/rueidis v1.0.63
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mateothegreat/go-config v0.0.0-20250727162039-eb3615a83541 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.11.0 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/spf13/pflag v1.0.7 // indirect
	golang.org/x/sys v0.34.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Replace with local modules for development
replace github.com/notioncodes/client => ../../client

replace github.com/notioncodes/types => ../../types
