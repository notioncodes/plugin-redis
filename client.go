// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"time"

	red "github.com/redis/go-redis/v9"
)

// Client is a wrapper around the rueidis client.
type Client struct {
	client *red.Client
	config *ClientConfig
}

// Ping pings the Redis server.
//
// Returns:
// - An error if the Redis server is not reachable.
func (c *Client) Ping() error {
	return c.client.Ping(context.Background()).Err()
}

// ClientConfig holds configuration for the Redis transformer.
type ClientConfig struct {
	// Redis connection settings
	Address  string `json:"address" yaml:"address"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Database int    `json:"database" yaml:"database"`

	// Key configuration
	KeyPrefix    string `json:"key_prefix" yaml:"key_prefix"`
	KeySeparator string `json:"key_separator" yaml:"key_separator"`

	// Data settings
	TTL         time.Duration `json:"ttl" yaml:"ttl"`
	PrettyJSON  bool          `json:"pretty_json" yaml:"pretty_json"`
	IncludeMeta bool          `json:"include_meta" yaml:"include_meta"`

	// Performance settings
	Pipeline     bool          `json:"pipeline" yaml:"pipeline"`
	BatchSize    int           `json:"batch_size" yaml:"batch_size"`
	MaxRetries   int           `json:"max_retries" yaml:"max_retries"`
	RetryBackoff time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
}

// defaultRedisConfig returns sensible defaults for Redis configuration.
func defaultRedisConfig() *ClientConfig {
	return &ClientConfig{
		Address:      "localhost:6379",
		Database:     0,
		KeyPrefix:    "notion",
		KeySeparator: ":",
		TTL:          24 * time.Hour,
		PrettyJSON:   false,
		IncludeMeta:  true,
		Pipeline:     true,
		BatchSize:    100,
		MaxRetries:   3,
		RetryBackoff: time.Second,
	}
}

// NewRedisClient creates a new Redis client instance.
//
// Arguments:
// - config: The Redis client configuration.
//
// Returns:
// - The Redis client instance.
// - An error if the Redis client creation fails.
func NewRedisClient(config *ClientConfig) (*Client, error) {
	client := red.NewClient(&red.Options{
		Addr:     config.Address,
		Username: config.Username,
		Password: config.Password,
		DB:       config.Database,
	})

	return &Client{
		client: client,
		config: config,
	}, nil
}
