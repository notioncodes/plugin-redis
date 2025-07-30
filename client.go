// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mateothegreat/go-multilog/multilog"
	"github.com/notioncodes/types"
	red "github.com/redis/go-redis/v9"
)

// Client is a wrapper around the rueidis client.
type Client struct {
	client *red.Client
	plugin *Pugin
	wg     sync.WaitGroup
	ch     chan StoreRequest
	ctx    context.Context
}

// StoreRequest is a request to store data in Redis.
type StoreRequest struct {
	ObjectType types.ObjectType
	Object     interface{}
	TTL        time.Duration
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
		c.plugin.Config.KeyPrefix,
		string(objectType),
		types.ExtractID(object),
	}, c.plugin.Config.KeySeparator)
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
func (c *Client) Store(ctx context.Context, req StoreRequest) (string, error) {
	var lastErr error

	for attempt := 0; attempt <= c.plugin.Config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff and then wait for that duration.
			backoff := time.Duration(attempt) * c.plugin.Config.RetryBackoff
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(backoff):
			}
		}

		str, err := json.Marshal(req.Object)
		if err != nil {
			return "", fmt.Errorf("failed to marshal object: %w", err)
		}

		key := c.Key(req.ObjectType, req.Object)

		if err := c.client.Set(ctx, key, str, req.TTL).Err(); err != nil {
			multilog.Error("redis.client.Store", "failed to set key in redis", map[string]interface{}{
				"key":        key,
				"objectType": req.ObjectType,
				"error":      err,
				"attempt":    attempt + 1,
			})
			lastErr = err
			continue
		}

		multilog.Trace("redis.client.Store", "stored object", map[string]interface{}{
			"key":        key,
			"objectType": req.ObjectType,
		})

		return key, nil
	}

	return "", fmt.Errorf("failed after %d attempts: %w", c.plugin.Config.MaxRetries+1, lastErr)
}

func (c *Client) Request(req StoreRequest) error {
	// Store directly instead of queuing to workers
	_, err := c.Store(c.ctx, req)
	return err
}

// worker is a process that listens on a channel for StoreRequests and stores
// data in Redis.
//
// Returns:
// - An error if the data could not be stored.
func (c *Client) worker(id int) error {
	defer c.wg.Done()

	multilog.Info("redis.client.worker", "worker started", map[string]interface{}{
		"id": id,
	})

	for req := range c.ch {
		multilog.Info("redis.client.worker", "processing request", map[string]interface{}{
			"id":         id,
			"objectType": req.ObjectType,
		})

		key, err := c.Store(c.ctx, req)
		if err != nil {
			multilog.Error("redis.client.worker", "failed to store data", map[string]interface{}{
				"id":    id,
				"key":   key,
				"type":  req.ObjectType,
				"error": err,
			})
			continue
		}

		multilog.Info("redis.client.worker", "successfully processed request", map[string]interface{}{
			"id":  id,
			"key": key,
		})
	}
	return nil
}

// CreateIndex creates an index for the given index name and index type.
func (c *Client) CreateIndex(ctx context.Context) error {
	idx := c.client.Do(ctx, c.client.FTCreate(ctx, c.plugin.Config.KeyPrefix, &red.FTCreateOptions{
		OnJSON: true,
		Prefix: []interface{}{c.plugin.Config.KeyPrefix},
	}, &red.FieldSchema{
		FieldName: "$.id",
		FieldType: red.SearchFieldTypeText,
		Sortable:  true,
		As:        "id",
	}))
	if idx.Err() != nil {
		return idx.Err()
	}
	return nil
}

// NewRedisClient creates a new Redis client instance.
//
// Arguments:
// - config: The Redis client configuration.
//
// Returns:
// - The Redis client instance.
// - An error if the Redis client creation fails.
func NewRedisClient(ctx context.Context, plugin *Pugin) (*Client, error) {
	client := red.NewClient(&red.Options{
		Addr:     plugin.Config.ClientConfig.Address,
		Username: plugin.Config.ClientConfig.Username,
		Password: plugin.Config.ClientConfig.Password,
		DB:       plugin.Config.ClientConfig.Database,
	})

	c := &Client{
		client: client,
		plugin: plugin,
		ctx:    ctx,
	}

	c.ch = make(chan StoreRequest, c.plugin.Config.BatchSize)

	for i := 0; i < c.plugin.Config.Common.Workers; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}
	return c, nil
}

// Close gracefully shuts down the Redis client and waits for all workers to finish.
func (c *Client) Close() error {
	close(c.ch)
	c.wg.Wait()
	return c.client.Close()
}

// Flush flushes the Redis database.
//
// Returns:
// - An error if the Redis database could not be flushed.
func (c *Client) Flush() error {
	return c.client.FlushAll(context.Background()).Err()
}
