// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/notioncodes/types"
)

type RedisTransformer struct {
	client *Client
	config *ClientConfig
}

// NewRedisTransformer creates a new Redis transformer instance.
func NewRedisTransformer() *RedisTransformer {
	return &RedisTransformer{}
}

// Name returns the transformer name.
func (t *RedisTransformer) Name() string {
	return "redis"
}

// Version returns the transformer version.
func (t *RedisTransformer) Version() string {
	return "1.0.0"
}

// SupportedTypes returns the object types this transformer can handle.
func (t *RedisTransformer) SupportedTypes() []types.ObjectType {
	return []types.ObjectType{
		types.ObjectTypePage,
		types.ObjectTypeDatabase,
		types.ObjectTypeBlock,
		types.ObjectTypeUser,
	}
}

// Priority returns the execution priority.
func (t *RedisTransformer) Priority() int {
	return 5
}

// Transform processes a Notion object and stores it in Redis.
func (t *RedisTransformer) Transform(ctx context.Context, objectType types.ObjectType, object interface{}) (interface{}, error) {
	if t.client == nil {
		return nil, fmt.Errorf("Redis client not initialized")
	}

	// Generate Redis key
	key, err := t.generateKey(objectType, object)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Redis key: %w", err)
	}

	// Prepare data for storage
	data, err := t.prepareData(objectType, object)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare data: %w", err)
	}

	// Store in Redis with retry logic
	if err := t.storeWithRetry(ctx, key, data); err != nil {
		return nil, fmt.Errorf("failed to store in Redis: %w", err)
	}

	// Return metadata about the storage operation
	result := map[string]interface{}{
		"redis_key":   key,
		"object_type": string(objectType),
		"stored_at":   time.Now(),
		"ttl_seconds": int64(t.config.TTL.Seconds()),
	}

	return result, nil
}

// Cleanup closes the Redis connection.
func (t *RedisTransformer) Cleanup() error {
	if t.client != nil {
		t.client.client.Close()
		t.client = nil
	}
	return nil
}

// generateKey creates a Redis key based on object type and content.
func (t *RedisTransformer) generateKey(objectType types.ObjectType, object interface{}) (string, error) {
	parts := []string{t.config.KeyPrefix, string(objectType)}

	// Extract ID based on object type
	var id string
	switch objectType {
	case types.ObjectTypePage:
		if page, ok := object.(*types.Page); ok && page != nil {
			id = page.ID.String()
		}
	case types.ObjectTypeDatabase:
		if db, ok := object.(*types.Database); ok && db != nil {
			id = db.ID.String()
		}
	case types.ObjectTypeBlock:
		if block, ok := object.(*types.Block); ok && block != nil {
			id = block.ID.String()
		}
	case types.ObjectTypeUser:
		if user, ok := object.(*types.User); ok && user != nil {
			id = user.ID.String()
		}
	}

	if id == "" {
		return "", fmt.Errorf("unable to extract ID from object of type %s", objectType)
	}

	parts = append(parts, id)
	return strings.Join(parts, t.config.KeySeparator), nil
}

// prepareData serializes the object for Redis storage.
func (t *RedisTransformer) prepareData(objectType types.ObjectType, object interface{}) ([]byte, error) {
	var data interface{} = object

	// Add metadata if configured
	if t.config.IncludeMeta {
		metadata := map[string]interface{}{
			"object_type":   string(objectType),
			"exported_at":   time.Now(),
			"transformer":   t.Name(),
			"version":       t.Version(),
			"original_data": object,
		}
		data = metadata
	}

	// Serialize to JSON
	if t.config.PrettyJSON {
		return json.MarshalIndent(data, "", "  ")
	}
	return json.Marshal(data)
}

// storeWithRetry stores data in Redis with exponential backoff retry logic.
func (t *RedisTransformer) storeWithRetry(ctx context.Context, key string, data []byte) error {
	var lastErr error

	for attempt := 0; attempt <= t.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff and then wait for that duration.
			backoff := time.Duration(attempt) * t.config.RetryBackoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		if err := t.client.client.Set(ctx, key, string(data), t.config.TTL).Err(); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("failed after %d attempts: %w", t.config.MaxRetries+1, lastErr)
}
