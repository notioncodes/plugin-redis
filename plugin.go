// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"

	"github.com/notioncodes/client"
	"github.com/notioncodes/plugin"
)

// Pugin is the plugin container.
type Pugin struct {
	Base         plugin.Base
	NotionClient *client.Client
	RedisClient  *Client
	Service      *Service
	Config       Config
	Flush        bool
}

// Config is the configuration container for this plugin.
type Config struct {
	// Embed the base plugin configuration.
	plugin.Config

	// Configuration for connecting to a redis
	ClientConfig

	Common  plugin.CommonSettings  `json:"general" yaml:"general"`
	Content plugin.ContentSettings `json:"content" yaml:"content"`
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
func NewPlugin(notionClient *client.Client, config Config) (*Pugin, error) {
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

	plugin := &Pugin{
		Base:         *plugin.NewPlugin(config.Config),
		NotionClient: notionClient,
		Config:       config,
		Flush:        config.Content.Flush,
	}

	var err error
	plugin.RedisClient, err = NewRedisClient(context.Background(), plugin)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
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
