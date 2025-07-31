// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/notioncodes/client"
	"github.com/notioncodes/types"
)

// Service provides high-level Redis export functionality with
// concurrent processing and batch operations for efficient Notion data export.
type Service struct {
	Plugin        *Plugin
	transformer   *Transformer
	exportService *client.ExportService
	result        *ExportResult
}

// NewService creates a new Redis service instance.
//
// Arguments:
// - plugin: The Redis plugin instance containing configuration and clients.
//
// Returns:
// - The Redis service instance.
// - An error if the Redis service creation fails.
func NewService(plugin *Plugin) (*Service, error) {
	exportService := client.NewExportService(plugin.NotionClient)

	return &Service{
		Plugin:        plugin,
		transformer:   NewTransformer(plugin),
		exportService: exportService,
		result:        NewExportResult(),
	}, nil
}

// ExportDatabase exports a database with all its pages, blocks, and comments to Redis.
//
// Arguments:
// - ctx: The context for the export operation.
// - databaseID: The ID of the database to export.
// - includePages: Whether to include pages from the database.
// - includeBlocks: Whether to include blocks from the pages.
// - includeComments: Whether to include comments from the blocks.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) ExportDatabase(ctx context.Context, databaseID types.DatabaseID, includePages, includeBlocks, includeComments bool) (*ExportResult, error) {
	s.result.Start = time.Now()
	defer func() {
		s.result.End = time.Now()
	}()

	// Configure export options
	opts := client.ExportDatabaseOptions{
		Pages: client.ExportPageOptions{
			Blocks: client.ExportBlockOptions{
				Children: includeBlocks,
				Comments: client.ExportCommentOptions{
					User: includeComments,
				},
			},
		},
	}

	// Only include pages if requested
	if !includePages {
		opts.Pages.Blocks.Children = false
		opts.Pages.Blocks.Comments.User = false
	}

	// Export database using the new client service
	result, err := s.exportService.ExportDatabase(ctx, databaseID, opts)
	if err != nil {
		return s.result, fmt.Errorf("failed to export database: %w", err)
	}

	// Process and store the exported data in Redis
	if err := s.storeDatabaseResult(ctx, result); err != nil {
		return s.result, fmt.Errorf("failed to store database result: %w", err)
	}

	return s.result, nil
}

// ExportPage exports a specific page with its blocks and comments to Redis.
//
// Arguments:
// - ctx: The context for the export operation.
// - pageID: The ID of the page to export.
// - includeBlocks: Whether to include blocks from the page.
// - includeComments: Whether to include comments from the blocks.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) ExportPage(ctx context.Context, pageID types.PageID, includeBlocks, includeComments bool) (*ExportResult, error) {
	s.result.Start = time.Now()
	defer func() {
		s.result.End = time.Now()
	}()

	// Configure export options
	opts := client.ExportPageOptions{
		Blocks: client.ExportBlockOptions{
			Children: includeBlocks,
			Comments: client.ExportCommentOptions{
				User: includeComments,
			},
		},
	}

	// Export page using the new client service
	result, err := s.exportService.ExportPage(ctx, pageID, opts)
	if err != nil {
		return s.result, fmt.Errorf("failed to export page: %w", err)
	}

	// Process and store the exported data in Redis
	if err := s.storePageResult(ctx, result); err != nil {
		return s.result, fmt.Errorf("failed to store page result: %w", err)
	}

	return s.result, nil
}

// ExportBlock exports a specific block with its children and comments to Redis.
//
// Arguments:
// - ctx: The context for the export operation.
// - blockID: The ID of the block to export.
// - includeChildren: Whether to include children of the block.
// - includeComments: Whether to include comments on the block.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) ExportBlock(ctx context.Context, blockID types.BlockID, includeChildren, includeComments bool) (*ExportResult, error) {
	s.result.Start = time.Now()
	defer func() {
		s.result.End = time.Now()
	}()

	// Configure export options
	opts := client.ExportBlockOptions{
		Children: includeChildren,
		Comments: client.ExportCommentOptions{
			User: includeComments,
		},
	}

	// Export block using the new client service
	result, err := s.exportService.ExportBlock(ctx, blockID, opts)
	if err != nil {
		return s.result, fmt.Errorf("failed to export block: %w", err)
	}

	// Process and store the exported data in Redis
	if err := s.storeBlockResult(ctx, result); err != nil {
		return s.result, fmt.Errorf("failed to store block result: %w", err)
	}

	return s.result, nil
}

// Export exports content based on the plugin configuration.
//
// Arguments:
// - ctx: The context for the export operation.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) Export(ctx context.Context, opts client.ExportOptions) (*ExportResult, error) {
	s.result.Start = time.Now()
	defer func() {
		s.result.End = time.Now()
	}()

	result, err := s.exportService.Export(ctx, opts)
	if err != nil {
		return s.result, fmt.Errorf("failed to export: %w", err)
	}

	for _, databaseResult := range result.Databases {
		if err := s.storeDatabaseResult(ctx, databaseResult); err != nil {
			return s.result, fmt.Errorf("failed to store database result: %w", err)
		}
	}

	// // Export databases if configured
	// for _, databaseID := range s.Plugin.Config.Content.Databases.IDs {
	// 	dbResult, err := s.ExportDatabase(ctx, databaseID,
	// 		s.Plugin.Config.Content.Databases.Pages,
	// 		s.Plugin.Config.Content.Databases.Blocks,
	// 		s.Plugin.Config.Content.Pages.Comments)
	// 	if err != nil {
	// 		if !s.Plugin.Config.Common.ContinueOnError {
	// 			return s.result, fmt.Errorf("failed to export database %s: %w", databaseID, err)
	// 		}
	// 		// Record error but continue
	// 		s.result.Errored(types.ObjectTypeDatabase, databaseID.String(), err)
	// 	} else {
	// 		// Merge results
	// 		s.result.Successful(types.ObjectTypeDatabase, dbResult)
	// 	}
	// }

	// // Export specific pages if configured
	// for _, pageID := range s.Plugin.Config.Content.Pages.IDs {
	// 	pageResult, err := s.ExportPage(ctx, pageID,
	// 		s.Plugin.Config.Content.Pages.Blocks,
	// 		s.Plugin.Config.Content.Pages.Comments)
	// 	if err != nil {
	// 		if !s.Plugin.Config.Common.ContinueOnError {
	// 			return s.result, fmt.Errorf("failed to export page %s: %w", pageID, err)
	// 		}
	// 		// Record error but continue
	// 		s.result.Errored(types.ObjectTypePage, pageID.String(), err)
	// 	} else {
	// 		// Merge results
	// 		s.result.Successful(types.ObjectTypePage, pageResult)
	// 	}
	// }

	return s.result, nil
}

// Storage methods for processing exported data

// storeDatabaseResult processes and stores a database export result in Redis.
func (s *Service) storeDatabaseResult(ctx context.Context, result *client.ExportDatabaseResult) error {
	// Store the database
	if err := s.storeObject(ctx, types.ObjectTypeDatabase, result.Database); err != nil {
		return fmt.Errorf("failed to store database: %w", err)
	}
	s.result.Success[types.ObjectTypeDatabase]++

	// Store all pages
	for _, pageResult := range result.Pages {
		if err := s.storePageResult(ctx, pageResult); err != nil {
			return fmt.Errorf("failed to store page result: %w", err)
		}
	}

	return nil
}

// storePageResult processes and stores a page export result in Redis.
func (s *Service) storePageResult(ctx context.Context, result *client.ExportPageResult) error {
	// Store the page
	if err := s.storeObject(ctx, types.ObjectTypePage, result.Page); err != nil {
		return fmt.Errorf("failed to store page: %w", err)
	}
	s.result.Success[types.ObjectTypePage]++

	// Store all blocks
	for _, blockResult := range result.Blocks {
		if err := s.storeBlockResult(ctx, blockResult); err != nil {
			return fmt.Errorf("failed to store block result: %w", err)
		}
	}

	return nil
}

// storeBlockResult processes and stores a block export result in Redis.
func (s *Service) storeBlockResult(ctx context.Context, result *client.ExportBlockResult) error {
	// Store the block
	if err := s.storeObject(ctx, types.ObjectTypeBlock, result.Block); err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}
	s.result.Success[types.ObjectTypeBlock]++

	// Store child blocks
	for _, childBlock := range result.Children {
		if err := s.storeObject(ctx, types.ObjectTypeBlock, childBlock); err != nil {
			return fmt.Errorf("failed to store child block: %w", err)
		}
		s.result.Success[types.ObjectTypeBlock]++
	}

	// Store comments
	for _, commentResult := range result.Comments {
		if err := s.storeCommentResult(ctx, commentResult); err != nil {
			return fmt.Errorf("failed to store comment result: %w", err)
		}
	}

	return nil
}

// storeCommentResult processes and stores a comment export result in Redis.
func (s *Service) storeCommentResult(ctx context.Context, result *client.ExportCommentResult) error {
	// Store the comment
	if err := s.storeObject(ctx, types.ObjectTypeComment, result.Comment); err != nil {
		return fmt.Errorf("failed to store comment: %w", err)
	}
	s.result.Success[types.ObjectTypeComment]++

	// Store user if included
	if result.User != nil {
		if err := s.storeObject(ctx, types.ObjectTypeUser, result.User); err != nil {
			return fmt.Errorf("failed to store user: %w", err)
		}
		s.result.Success[types.ObjectTypeUser]++
	}

	return nil
}

// storeObject transforms and stores a single object in Redis.
func (s *Service) storeObject(ctx context.Context, objectType types.ObjectType, obj interface{}) error {
	// Transform the object
	data, err := s.transformer.Transform(ctx, objectType, obj)
	if err != nil {
		return fmt.Errorf("failed to transform object: %w", err)
	}

	// Store in Redis
	err = s.Plugin.RedisClient.Request(StoreRequest{
		ObjectType: objectType,
		Object:     data,
		TTL:        s.Plugin.Config.ClientConfig.TTL,
	})
	if err != nil {
		return fmt.Errorf("failed to store in Redis: %w", err)
	}

	return nil
}

// Close cleans up the export service.
func (s *Service) Close() error {
	// Clear user cache
	s.exportService.ClearUserCache()

	// Close transformer
	return s.transformer.Cleanup()
}

// ExportError represents an error that occurred during export.
type ExportError struct {
	ObjectType types.ObjectType `json:"object_type"`
	ObjectID   string           `json:"object_id"`
	Error      string           `json:"error"`
	Timestamp  time.Time        `json:"timestamp"`
}

// NewExportResult creates a new ExportResult.
func NewExportResult() *ExportResult {
	return &ExportResult{
		Start:   time.Now(),
		Success: make(map[types.ObjectType]int),
		Errors:  []ExportError{},
	}
}

// ExportResult contains the results of an export operation.
type ExportResult struct {
	Start    time.Time                `json:"start"`
	End      time.Time                `json:"end"`
	Success  map[types.ObjectType]int `json:"success"`
	Requests int                      `json:"requests"`
	Errors   []ExportError            `json:"errors,omitempty"`
	mu       sync.RWMutex
}

// Errored adds an error to the export result.
//
// Arguments:
// - objectType: The type of object that errored.
// - objectID: The ID of the object that errored.
// - err: The error that occurred.
func (e *ExportResult) Errored(objectType types.ObjectType, objectID string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Errors = append(e.Errors, ExportError{
		ObjectType: objectType,
		ObjectID:   objectID,
		Error:      err.Error(),
		Timestamp:  time.Now(),
	})
}

// Successful merges another export result into this one.
//
// Arguments:
// - objectType: The type of object that was exported.
// - r: The export result to merge.
func (e *ExportResult) Successful(objectType types.ObjectType, r *ExportResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Merge success counts
	for objType, count := range r.Success {
		e.Success[objType] += count
	}

	// Merge errors
	e.Errors = append(e.Errors, r.Errors...)
}

// Total returns the total number of objects processed (successful + errors).
//
// Returns:
// - The total number of objects processed.
func (e *ExportResult) Total() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int
	for _, count := range e.Success {
		total += count
	}

	return total + len(e.Errors)
}
