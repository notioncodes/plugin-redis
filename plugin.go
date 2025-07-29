// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"fmt"
	"time"

	"github.com/notioncodes/client"
	"github.com/notioncodes/plugin"
	"github.com/notioncodes/types"
)

// RedisPlugin is the plugin container.
type RedisPlugin struct {
	Base         plugin.Base
	NotionClient *client.Client
	RedisClient  *Client
	Service      *Service
	Config       Config
}

// Config is the configuration container for this plugin.
type Config struct {
	// Embed the base plugin configuration.
	plugin.BaseConfig

	// Configuration for connecting to a redis
	ClientConfig

	// Concurrency settings
	Workers   int           `json:"workers" yaml:"workers"`
	BatchSize int           `json:"batch_size" yaml:"batch_size"`
	Timeout   time.Duration `json:"timeout" yaml:"timeout"`
	RateLimit int           `json:"rate_limit" yaml:"rate_limit"` // requests per second

	// Export options
	ObjectTypes   []types.ObjectType `json:"object_types" yaml:"object_types"`
	IncludeBlocks bool               `json:"include_blocks" yaml:"include_blocks"`

	// Progress tracking
	EnableProgress    bool          `json:"enable_progress" yaml:"enable_progress"`
	ProgressInterval  time.Duration `json:"progress_interval" yaml:"progress_interval"`     // Time-based debouncing
	ProgressBatchSize int           `json:"progress_batch_size" yaml:"progress_batch_size"` // Count-based debouncing

	// Error handling
	ContinueOnError bool `json:"continue_on_error" yaml:"continue_on_error"`
	MaxErrors       int  `json:"max_errors" yaml:"max_errors"`
}

// NewPlugin creates a new Redis plugin.
//
// Arguments:
// - client: Instance of the Notion client from the `github.com/notioncodes/client` package.
// - redisConfig: The Redis configuration.
//
// Returns:
// - The Redis plugin instance.
// - An error if plugin initialization fails.
func NewPlugin(notionClient *client.Client, config Config) (*RedisPlugin, error) {
	// pluginConfig := &PluginConfig{
	// 	Workers:         4,
	// 	BatchSize:       50,
	// 	Timeout:         10 * time.Second, // Reduced timeout for faster operations
	// 	RateLimit:       100,
	// 	ObjectTypes:     []types.ObjectType{types.ObjectTypePage, types.ObjectTypeDatabase},
	// 	IncludeBlocks:   true,
	// 	ContinueOnError: true,
	// 	MaxErrors:       10,
	// }

	// Create a new Redis client
	redisClient, err := NewRedisClient(&config.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}

	plugin := &RedisPlugin{
		Base:         *plugin.NewPlugin(config.BaseConfig),
		NotionClient: notionClient,
		RedisClient:  redisClient,
		Config:       config,
	}

	if err := plugin.RedisClient.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	// Create a new Redis service
	plugin.Service, err = NewService(plugin)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis service: %w", err)
	}

	return plugin, nil
}
