// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	red "github.com/redis/go-redis/v9"

	"github.com/notioncodes/client"
	"github.com/notioncodes/types"
)

// Service provides high-level Redis export functionality with
// concurrent processing and batch operations for efficient Notion data export.
type Service struct {
	Plugin      *RedisPlugin
	transformer *RedisTransformer
}

// NewService creates a new Redis service instance.
//
// Arguments:
// - config: The Redis service configuration.
//
// Returns:
// - The Redis service instance.
// - An error if the Redis service creation fails.
func NewService(plugin *RedisPlugin) (*Service, error) {
	transformer := NewRedisTransformer()

	return &Service{
		Plugin:      plugin,
		transformer: transformer,
	}, nil
}

// ExportAll exports all accessible Notion content to Redis.
func (s *Service) ExportAll(ctx context.Context) (*ExportResult, error) {
	result := NewExportResult()

	// Get all databases first.
	databases, err := s.getAllDatabases(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to get databases: %w", err)
	}

	// Export databases.
	if contains(s.Plugin.Config.ObjectTypes, types.ObjectTypeDatabase) {
		dbResult, err := s.exportDatabases(ctx, databases)
		if err != nil && !s.Plugin.Config.ContinueOnError {
			return result, fmt.Errorf("failed to export databases: %w", err)
		}
		s.mergeResults(result, dbResult)
	}

	// Export pages from each database.
	if contains(s.Plugin.Config.ObjectTypes, types.ObjectTypePage) {
		for _, db := range databases {
			pages, err := s.getPagesFromDatabase(ctx, db.ID)
			if err != nil {
				if !s.Plugin.Config.ContinueOnError {
					return result, fmt.Errorf("failed to get pages from database %s: %w", db.ID, err)
				}
				result.Errors = append(result.Errors, ExportError{
					ObjectType: types.ObjectTypeDatabase,
					ObjectID:   db.ID.String(),
					Error:      err.Error(),
					Timestamp:  time.Now(),
				})
				continue
			}

			pageResult, err := s.exportPages(ctx, pages)
			if err != nil && !s.Plugin.Config.ContinueOnError {
				return result, fmt.Errorf("failed to export pages from database %s: %w", db.ID, err)
			}
			s.mergeResults(result, pageResult)

			// Export blocks from pages if configured.
			if s.Plugin.Config.IncludeBlocks && contains(s.Plugin.Config.ObjectTypes, types.ObjectTypeBlock) {
				for _, page := range pages {
					blockResult, err := s.exportPageBlocks(ctx, page.ID)
					if err != nil && !s.Plugin.Config.ContinueOnError {
						return result, fmt.Errorf("failed to export blocks from page %s: %w", page.ID, err)
					}
					s.mergeResults(result, blockResult)
				}
			}
		}
	}

	result.End = time.Now()

	return result, nil
}

// ExportDatabase exports a specific database and optionally its pages to Redis.
func (s *Service) ExportDatabase(ctx context.Context, databaseID types.DatabaseID, includePages bool) (*ExportResult, error) {
	result := NewExportResult()

	// Get database.
	db, err := s.getDatabase(ctx, databaseID)
	if err != nil {
		return result, fmt.Errorf("failed to get database: %w", err)
	}

	// Export database.
	dbResult, err := s.exportObjects(ctx, []interface{}{db}, types.ObjectTypeDatabase)
	if err != nil {
		return result, fmt.Errorf("failed to export database: %w", err)
	}
	s.mergeResults(result, dbResult)

	// Export pages if requested.
	if includePages {
		pages, err := s.getPagesFromDatabase(ctx, databaseID)
		if err != nil {
			return result, fmt.Errorf("failed to get pages: %w", err)
		}

		pageResult, err := s.exportPages(ctx, pages)
		if err != nil {
			return result, fmt.Errorf("failed to export pages: %w", err)
		}
		s.mergeResults(result, pageResult)
	}

	result.End = time.Now()

	return result, nil
}

// ExportPage exports a specific page and optionally its blocks to Redis.
func (s *Service) ExportPage(ctx context.Context, pageID types.PageID, includeBlocks bool) (*ExportResult, error) {
	result := NewExportResult()

	// Get page.
	page, err := s.getPage(ctx, pageID)
	if err != nil {
		return result, fmt.Errorf("failed to get page: %w", err)
	}

	// Export page.
	pageResult, err := s.exportObjects(ctx, []interface{}{page}, types.ObjectTypePage)
	if err != nil {
		return result, fmt.Errorf("failed to export page: %w", err)
	}
	s.mergeResults(result, pageResult)

	// Export blocks if requested.
	if includeBlocks {
		blockResult, err := s.exportPageBlocks(ctx, pageID)
		if err != nil {
			return result, fmt.Errorf("failed to export blocks: %w", err)
		}
		s.mergeResults(result, blockResult)
	}

	result.End = time.Now()

	return result, nil
}

// exportObjects exports a slice of objects concurrently.
func (s *Service) exportObjects(ctx context.Context, objects []interface{}, objectType types.ObjectType) (*ExportResult, error) {
	result := NewExportResult()

	if len(objects) == 0 {
		return result, nil
	}

	// Create worker pool.
	objectChan := make(chan interface{}, len(objects))
	resultChan := make(chan exportWorkerResult, len(objects))

	// Start workers.
	var wg sync.WaitGroup
	for i := 0; i < s.Plugin.Config.Workers; i++ {
		wg.Add(1)
		go s.exportWorker(ctx, objectChan, resultChan, objectType, &wg)
	}

	// Send objects to workers.
	for _, obj := range objects {
		select {
		case objectChan <- obj:
		case <-ctx.Done():
			close(objectChan)
			return result, ctx.Err()
		}
	}
	close(objectChan)

	// Wait for workers to complete.
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for workerResult := range resultChan {
		if workerResult.Error != nil {
			result.Errors = append(result.Errors, ExportError{
				ObjectType: objectType,
				ObjectID:   workerResult.ObjectID,
				Error:      workerResult.Error.Error(),
				Timestamp:  time.Now(),
			})

			if len(result.Errors) >= s.Plugin.Config.MaxErrors {
				return result, fmt.Errorf("max errors (%d) reached", s.Plugin.Config.MaxErrors)
			}
		} else {
			result.Success++
		}

		s.Plugin.Base.Reporter.Report(len(objects), result.Success+len(result.Errors), objectType, workerResult.Object)
	}

	return result, nil
}

// exportWorker processes objects from the channel and exports them to Redis.
func (s *Service) exportWorker(ctx context.Context, objectChan <-chan interface{}, resultChan chan<- exportWorkerResult, objectType types.ObjectType, wg *sync.WaitGroup) {
	defer wg.Done()

	for object := range objectChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Extract object ID for error reporting.
		var objectID string
		switch objectType {
		case types.ObjectTypePage:
			if page, ok := object.(*types.Page); ok {
				objectID = page.ID.String()
			}
		case types.ObjectTypeDatabase:
			if db, ok := object.(*types.Database); ok {
				objectID = db.ID.String()
			}
		case types.ObjectTypeBlock:
			if block, ok := object.(*types.Block); ok {
				objectID = block.ID.String()
			}
		}

		// Transform object.
		_, err := s.transformer.Transform(ctx, objectType, object)

		resultChan <- exportWorkerResult{
			ObjectID: objectID,
			Object:   object,
			Error:    err,
		}
	}
}

// Helper methods for data retrieval.

func (s *Service) getAllDatabases(ctx context.Context) ([]*types.Database, error) {
	startTime := time.Now()
	defer func() {
		fmt.Printf("[DEBUG] getAllDatabases took %v\n", time.Since(startTime))
	}()

	// Create a timeout context for the search operation.
	searchCtx, cancel := context.WithTimeout(ctx, s.Plugin.Config.Timeout)
	defer cancel()

	ch := s.Plugin.NotionClient.Search().Query(searchCtx, types.SearchRequest{
		Query: "",
		Filter: &types.SearchFilter{
			Property: "object",
			Value:    "database",
		},
	})

	var databases []*types.Database
	for result := range ch {
		if result.Error != nil {
			if result.Error == context.DeadlineExceeded {
				return databases, fmt.Errorf("search timeout after %v", s.Plugin.Config.Timeout)
			}
			return databases, result.Error
		}
		if result.Data.Database != nil {
			databases = append(databases, result.Data.Database)
		}
	}

	return databases, nil
}

func (s *Service) getDatabase(ctx context.Context, databaseID types.DatabaseID) (*types.Database, error) {
	result, err := client.ToGoResult(s.Plugin.NotionClient.Databases().Get(ctx, databaseID))
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Service) getPagesFromDatabase(ctx context.Context, databaseID types.DatabaseID) ([]*types.Page, error) {
	startTime := time.Now()
	defer func() {
		fmt.Printf("[DEBUG] getPagesFromDatabase(%s) took %v\n", databaseID, time.Since(startTime))
	}()

	// Create a timeout context for the database query.
	queryCtx, cancel := context.WithTimeout(ctx, s.Plugin.Config.Timeout)
	defer cancel()

	ch := s.Plugin.NotionClient.Databases().Query(queryCtx, databaseID, nil, nil)

	var pages []*types.Page
	for result := range ch {
		if result.Error != nil {
			if result.Error == context.DeadlineExceeded {
				return pages, fmt.Errorf("database query timeout after %v", s.Plugin.Config.Timeout)
			}
			return pages, result.Error
		}
		pages = append(pages, &result.Data)
	}

	return pages, nil
}

func (s *Service) getPage(ctx context.Context, pageID types.PageID) (*types.Page, error) {
	result, err := client.ToGoResult(s.Plugin.NotionClient.Pages().Get(ctx, pageID))
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Service) exportDatabases(ctx context.Context, databases []*types.Database) (*ExportResult, error) {
	objects := make([]interface{}, len(databases))
	for i, db := range databases {
		objects[i] = db
	}
	return s.exportObjects(ctx, objects, types.ObjectTypeDatabase)
}

func (s *Service) exportPages(ctx context.Context, pages []*types.Page) (*ExportResult, error) {
	objects := make([]interface{}, len(pages))
	for i, page := range pages {
		objects[i] = page
	}
	return s.exportObjects(ctx, objects, types.ObjectTypePage)
}

// exportPageBlocks exports all blocks from a specific page.
// Note: This is a placeholder implementation. The current client API doesn't provide
// a direct method to get blocks for a page. This would need to be implemented
// based on the specific client API structure.
func (s *Service) exportPageBlocks(_ context.Context, pageID types.PageID) (*ExportResult, error) {
	// TODO: Implement block retrieval when client API supports it.
	// For now, return an empty result to avoid breaking the export flow.
	result := NewExportResult()
	result.Errors = append(result.Errors, ExportError{
		ObjectType: types.ObjectTypeBlock,
		ObjectID:   pageID.String(),
		Error:      "Block export not yet implemented - client API limitation",
		Timestamp:  time.Now(),
	})
	return result, nil
}

// mergeResults combines two export results.
func (s *Service) mergeResults(target, source *ExportResult) {
	target.Success += source.Success
	target.Errors = append(target.Errors, source.Errors...)
}

// CreateIndex creates an index for the given index name and index type.
func (s *Service) CreateIndex(ctx context.Context) error {
	idx := s.Plugin.RedisClient.client.Do(ctx, s.Plugin.RedisClient.client.FTCreate(ctx, "notion:page", &red.FTCreateOptions{
		OnJSON: true,
		Prefix: []interface{}{"notion:page"},
	}, &red.FieldSchema{
		FieldName: "$.title",
		FieldType: red.SearchFieldTypeText,
		Sortable:  true,
		As:        "title",
	}))
	if idx.Err() != nil {
		return idx.Err()
	}
	return nil
}

// Close cleans up the export service.
func (s *Service) Close() error {
	return s.transformer.Cleanup()
}

// ExportError represents an error that occurred during export.
type ExportError struct {
	ObjectType types.ObjectType `json:"object_type"`
	ObjectID   string           `json:"object_id"`
	Error      string           `json:"error"`
	Timestamp  time.Time        `json:"timestamp"`
}

// exportWorkerResult represents the result of a worker operation.
type exportWorkerResult struct {
	ObjectID string
	Object   interface{}
	Error    error
}

// ExportResult contains the results of an export operation.
type ExportResult struct {
	Start   time.Time     `json:"start"`
	End     time.Time     `json:"end"`
	Success int           `json:"success"`
	Errors  []ExportError `json:"errors,omitempty"`
}

// NewExportResult creates a new ExportResult.
func NewExportResult() *ExportResult {
	return &ExportResult{
		Start:   time.Now(),
		Success: 0,
		Errors:  []ExportError{},
	}
}
