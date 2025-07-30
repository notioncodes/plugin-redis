// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
	"log"

	"github.com/notioncodes/client"
	"github.com/notioncodes/plugin"
	"github.com/notioncodes/test"
)

// Plugin is the plugin container.
type Plugin struct {
	Base         plugin.Base
	NotionClient *client.Client
	RedisClient  *Client
	Service      *Service
	Config       Config
	Flush        bool
}

// Config is the configuration container for this plugin.
type Config struct {
	plugin.Config
	ClientConfig
	Common  plugin.CommonSettings  `json:"general" yaml:"general"`
	Content plugin.ContentSettings `json:"content" yaml:"content"`
}

// NewPlugin creates a new Redis plugin.
//
// Arguments:
// - config: The Redis plugin configuration.
//
// Returns:
// - The Redis plugin instance.
// - An error if plugin initialization fails.
func NewPlugin(pluginConfig Config) (*Plugin, error) {
	notionClient, err := client.NewClient(&client.Config{
		APIKey:        test.TestConfig.NotionAPIKey,
		EnableMetrics: true,
		RequestDelay:  pluginConfig.Common.RequestDelay,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer notionClient.Close()

	plugin := &Plugin{
		Base:         *plugin.NewPlugin(pluginConfig.Config),
		NotionClient: notionClient,
		Config:       pluginConfig,
		Flush:        pluginConfig.Content.Flush,
	}

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
