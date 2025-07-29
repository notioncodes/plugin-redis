// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/notioncodes/types"
	red "github.com/redis/go-redis/v9"
)

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

// ClientConfig holds configuration for the Redis client.
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

// Key generates a string intended to be used as a Key for redis storage.
// The Key is formatted as `<key_prefix>:<object_type>:<object_id>`.
//
// Arguments:
// - objectType: The type of object.
// - object: The object to generate a Key for.
//
// Returns:
// - The Redis Key string.
func (c *Client) Key(objectType types.ObjectType, object interface{}) string {
	return strings.Join([]string{
		c.config.KeyPrefix,
		string(objectType),
		types.ExtractID(object),
	}, c.config.KeySeparator)
}

// Store stores data in Redis with exponential backoff retry logic.
//
// Arguments:
// - ctx: The context for the operation.
// - key: The key to Store the data at.
// - data: The data to Store.
//
// Returns:
// - An error if the data could not be stored.
func (c *Client) Store(ctx context.Context, key string, data []byte) error {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff and then wait for that duration.
			backoff := time.Duration(attempt) * c.config.RetryBackoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		if err := c.client.Set(ctx, key, string(data), c.config.TTL).Err(); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}
