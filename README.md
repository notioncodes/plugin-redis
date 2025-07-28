# Redis Export Plugin

A high-performance Redis export plugin for the Notion client that enables efficient caching and fast access to Notion content through Redis.

## Features

- **High-performance exports** with concurrent processing and worker pools
- **Flexible configuration** including TTL, key strategies, and batch processing
- **Complete object support** for Pages, Databases, Blocks, and Users
- **Error handling** with configurable retry logic and error thresholds
- **Progress tracking** with real-time callbacks
- **Frontend integration** examples for Astro, SvelteKit, and other frameworks
- **100% test coverage** with comprehensive unit and integration tests

## Installation

```go
go get github.com/notioncodes/plugin-redis
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    "github.com/notioncodes/client"
    "github.com/notioncodes/plugin-redis"
)

func main() {
    // Create Notion client
    client, err := client.New(&client.Config{
        Token: "your_notion_token",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Redis configuration
    redisConfig := map[string]interface{}{
        "address":       "localhost:6379",
        "database":      0,
        "key_prefix":    "notion",
        "ttl":          "1h",
        "include_meta":  true,
    }

    // Create export service
    service, err := main.NewRedisExportService(client, redisConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer service.Close()

    // Export all content
    ctx := context.Background()
    result, err := service.ExportAll(ctx, nil)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Exported %d objects successfully", result.SuccessCount)
}
```

## Configuration

### Redis Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `address` | string | `localhost:6379` | Redis server address |
| `username` | string | | Redis username |
| `password` | string | | Redis password |
| `database` | int | `0` | Redis database number |
| `key_prefix` | string | `notion` | Prefix for all Redis keys |
| `key_separator` | string | `:` | Separator for Redis keys |
| `ttl` | duration | `24h` | Time-to-live for cached objects |
| `pretty_json` | bool | `false` | Format JSON with indentation |
| `include_meta` | bool | `true` | Include export metadata |
| `max_retries` | int | `3` | Maximum retry attempts |
| `retry_backoff` | duration | `1s` | Backoff duration between retries |

### Export Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | int | `4` | Number of concurrent workers |
| `batch_size` | int | `50` | Objects per batch |
| `timeout` | duration | `30s` | Operation timeout |
| `rate_limit` | int | `100` | Requests per second |
| `object_types` | []ObjectType | `[page, database]` | Types to export |
| `include_blocks` | bool | `true` | Export page blocks |
| `enable_progress` | bool | `false` | Enable progress callbacks |
| `continue_on_error` | bool | `true` | Continue after errors |
| `max_errors` | int | `10` | Maximum error threshold |

## Usage Examples

### Export Specific Database

```go
databaseID := types.DatabaseID("your-database-id")
result, err := service.ExportDatabase(ctx, databaseID, true, progressCallback)
```

### Export Specific Page

```go
pageID := types.PageID("your-page-id")
result, err := service.ExportPage(ctx, pageID, true, progressCallback)
```

### Progress Tracking

```go
progressCallback := func(processed, total int, objectType types.ObjectType) {
    percentage := float64(processed) / float64(total) * 100
    fmt.Printf("Progress: %s - %d/%d (%.1f%%)\n", objectType, processed, total, percentage)
}

result, err := service.ExportAll(ctx, progressCallback)
```

## Frontend Integration

### Key Pattern

Redis keys follow the pattern: `{prefix}:{object_type}:{object_id}`

Examples:
- `notion:page:abc123` - A page object  
- `notion:database:xyz789` - A database object
- `notion:block:def456` - A block object

### Astro Example

```javascript
// src/lib/notion.js
import Redis from 'ioredis';

const redis = new Redis({
  host: 'localhost',
  port: 6379,
  db: 0,
});

export async function getNotionPage(pageId) {
  const key = `notion:page:${pageId}`;
  const data = await redis.get(key);
  return data ? JSON.parse(data) : null;
}
```

### SvelteKit Example

```javascript
// src/routes/blog/+page.server.js
import { redis } from '$lib/redis.js';

export async function load() {
  const keys = await redis.keys('notion:page:*');
  const pages = await Promise.all(
    keys.map(async (key) => {
      const data = await redis.get(key);
      return JSON.parse(data);
    })
  );
  
  return { pages };
}
```

## Architecture

### Components

1. **RedisExportService** - High-level export orchestration
2. **RedisTransformer** - Low-level Redis storage operations  
3. **Worker Pool** - Concurrent processing system
4. **Configuration** - Flexible parameter management
5. **Error Handling** - Retry logic and error thresholds

### Data Flow

```
Notion API → Client → Export Service → Worker Pool → Redis Transformer → Redis
```

## Testing

Run tests with full coverage:

```bash
go test -v ./...
go test -cover ./...
```

For integration tests with actual Redis:

```bash
# Start Redis
docker run -d -p 6379:6379 redis:alpine

# Run integration tests
go test -v ./... -tags=integration
```

## Performance

- **Concurrent processing** with configurable worker pools
- **Batch operations** for optimal throughput
- **Connection pooling** with rueidis client
- **Exponential backoff** retry strategy
- **Memory-efficient** streaming operations

### Benchmarks

```bash
go test -bench=. ./...
```

## Error Handling

The plugin provides comprehensive error handling:

- **Connection failures** - Automatic retry with backoff
- **Rate limiting** - Built-in request throttling  
- **Timeout handling** - Configurable operation timeouts
- **Partial failures** - Continue processing on errors
- **Error thresholds** - Stop processing after max errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.