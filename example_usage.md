# Redis Plugin - Updated Export Service Usage

This document shows how to use the updated Redis plugin with the new export service API.

## Key Changes

The Redis plugin now uses the new `client.ExportService` API which provides:

- **Clean hierarchical export**: Database → Pages → Blocks → Comments → Users
- **Structured options**: Each object type has its own options structure
- **Better error handling**: Comments/users don't fail entire exports
- **Performance improvements**: User caching and concurrent processing

## Usage Examples

### Export a Database with All Data

```go
// Create a Redis plugin
config := redis.Config{
    // ... configuration
}
plugin, err := redis.NewPlugin(config)
if err != nil {
    log.Fatal(err)
}

// Export database with pages, blocks, and comments
result, err := plugin.Service.ExportDatabase(ctx, databaseID, 
    true,  // include pages
    true,  // include blocks  
    true,  // include comments with users
)
```

### Export a Single Page

```go
// Export page with blocks but no comments
result, err := plugin.Service.ExportPage(ctx, pageID,
    true,  // include blocks
    false, // don't include comments
)
```

### Export a Single Block

```go
// Export block with children and comments
result, err := plugin.Service.ExportBlock(ctx, blockID,
    true, // include children
    true, // include comments with users
)
```

### Configuration-Based Export

```go
// Export based on plugin configuration
result, err := plugin.Service.Export(ctx)
```

## Configuration Structure

The plugin configuration uses the standard plugin settings:

```yaml
content:
  databases:
    ids: ["database-id-1", "database-id-2"]
    pages: true    # Include pages from databases
    blocks: true   # Include blocks from pages
  pages:
    ids: ["page-id-1"] # Specific pages to export
    blocks: true       # Include blocks from pages
    comments: true     # Include comments from blocks
  blocks:
    children: true     # Include child blocks
```

## Storage Flow

1. **Export**: Uses the new `client.ExportService` to retrieve hierarchical data
2. **Transform**: Processes each object through the transformer
3. **Store**: Saves transformed objects to Redis with configured TTL

## Error Handling

- **Graceful degradation**: Comment/user failures don't stop the export
- **Detailed error reporting**: Each error includes object type, ID, and timestamp
- **Configurable**: Can continue on error or fail fast based on configuration

## Performance Features

- **User caching**: Avoids duplicate user API calls across the export
- **Concurrent processing**: Uses worker pools for large datasets  
- **Memory efficient**: Streams results instead of loading everything in memory
- **Redis optimized**: Batches storage operations and uses efficient key structures