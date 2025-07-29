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
	transformer *Transformer
	result      *ExportResult
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
	return &Service{
		Plugin:      plugin,
		transformer: NewTransformer(plugin),
		result:      NewExportResult(),
	}, nil
}

// ExportAll exports all accessible Notion content to Redis.
//
// Arguments:
// - ctx: The context for the export operation.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) ExportAll(ctx context.Context) (*ExportResult, error) {
	// Export databases.
	databases, err := s.getAllDatabases(ctx)
	if err != nil {
		return s.result, fmt.Errorf("failed to get databases: %w", err)
	}

	// // Export databases.
	// if contains(s.Plugin.Config.ObjectTypes, types.ObjectTypeDatabase) {
	// 	dbResult, err := s.exportDatabases(ctx, databases)
	// 	if err != nil && !s.Plugin.Config.ContinueOnError {
	// 		return s.result, fmt.Errorf("failed to export databases: %w", err)
	// 	}
	// 	s.mergeResults(s.result, dbResult)
	// }

	// Export pages from each database if enabled.
	if contains(s.Plugin.Config.ObjectTypes, types.ObjectTypePage) {
		for _, db := range databases {
			pages, err := s.getPagesFromDatabase(ctx, db.ID)
			if err != nil {
				s.result.Errored(types.ObjectTypeDatabase, db.ID.String(), err)
				if !s.Plugin.Config.ContinueOnError {
					return s.result, fmt.Errorf("failed to get pages from database %s: %w", db.ID, err)
				}
				continue
			}

			pageResult, err := s.exportPages(ctx, pages)
			if err != nil && !s.Plugin.Config.ContinueOnError {
				return s.result, fmt.Errorf("failed to export pages from database %s: %w", db.ID, err)
			}
			s.result.Successful(types.ObjectTypePage, pageResult)

			// Export blocks from pages if enabled.
			if s.Plugin.Config.IncludeBlocks && contains(s.Plugin.Config.ObjectTypes, types.ObjectTypeBlock) {
				for _, page := range pages {
					blockResult, err := s.exportPageBlocks(ctx, page.ID)
					if err != nil {
						s.result.Errored(types.ObjectTypeBlock, page.ID.String(), err)
						if !s.Plugin.Config.ContinueOnError {
							return s.result, fmt.Errorf("failed to export blocks from page %s: %w", page.ID, err)
						}
						continue
					}
					s.result.Successful(types.ObjectTypeBlock, blockResult)
				}
			}
		}
	}

	s.result.End = time.Now()

	return s.result, nil
}

// ExportDatabase exports a single database.
//
// Arguments:
// - ctx: The context for the export operation.
// - databaseID: The ID of the database to export.
// - includePages: Whether to include pages in the export.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) ExportDatabase(ctx context.Context, databaseID types.DatabaseID, includePages bool) (*ExportResult, error) {
	defer func() {
		s.result.End = time.Now()
	}()

	db, err := s.getDatabase(ctx, databaseID)
	if err != nil {
		return s.result, fmt.Errorf("failed to get database: %w", err)
	}

	// Export the database.
	res, err := s.schedule(ctx, []interface{}{db}, types.ObjectTypeDatabase)
	if err != nil {
		return s.result, fmt.Errorf("failed to export database: %w", err)
	}
	s.result.Successful(types.ObjectTypeDatabase, res)

	// Export pages if requested.
	if includePages {
		pages, err := s.getPagesFromDatabase(ctx, databaseID)
		if err != nil {
			return s.result, fmt.Errorf("failed to get pages: %w", err)
		}

		res, err := s.exportPages(ctx, pages)
		if err != nil {
			return s.result, fmt.Errorf("failed to export pages: %w", err)
		}
		s.result.Successful(types.ObjectTypePage, res)
	}

	return s.result, nil
}

// ExportPage exports a specific page and optionally its blocks to Redis.
func (s *Service) ExportPage(ctx context.Context, pageID types.PageID, includeBlocks bool) (*ExportResult, error) {
	defer func() {
		s.result.End = time.Now()
	}()

	// Get page.
	page, err := s.getPage(ctx, pageID)
	if err != nil {
		return s.result, fmt.Errorf("failed to get page: %w", err)
	}

	// Export page.
	res, err := s.schedule(ctx, []interface{}{page}, types.ObjectTypePage)
	if err != nil {
		return s.result, fmt.Errorf("failed to export page: %w", err)
	}
	s.result.Successful(types.ObjectTypePage, res)

	// Export blocks if requested.
	if includeBlocks {
		res, err := s.exportPageBlocks(ctx, pageID)
		if err != nil {
			return s.result, fmt.Errorf("failed to export blocks: %w", err)
		}
		s.result.Successful(types.ObjectTypeBlock, res)
	}

	return s.result, nil
}

// schedule exports a slice of objects concurrently by sending them to workers goroutines.
//
// Arguments:
// - ctx: The context for the export operation.
// - objects: The objects to export.
// - objectType: The type of objects to export.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) schedule(ctx context.Context, objects []interface{}, objectType types.ObjectType) (*ExportResult, error) {
	result := NewExportResult()

	objCh := make(chan interface{}, len(objects))
	resCh := make(chan workResult, len(objects))
	wg := sync.WaitGroup{}

	// Start all workers.
	for i := 0; i < s.Plugin.Config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range objCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				data, err := s.transformer.Transform(ctx, objectType, obj)
				if err != nil {
					resCh <- workResult{
						Object: obj,
						Error:  err,
					}
					continue
				}

				key := s.Plugin.RedisClient.Key(objectType, obj)
				if err := s.Plugin.RedisClient.Store(ctx, key, data); err != nil {
					resCh <- workResult{
						Key:    key,
						Object: obj,
						Error:  err,
					}
					continue
				}

				resCh <- workResult{Key: key, Object: obj}
			}
		}()
	}

	// Enqueue objects and let the channel handle distribution.
	for _, obj := range objects {
		select {
		case objCh <- obj:
		case <-ctx.Done():
			close(objCh)
			wg.Wait()
			close(resCh)
			return result, ctx.Err()
		}
	}
	close(objCh)

	// Close result channel after workers complete.
	go func() {
		wg.Wait()
		close(resCh)
	}()

	for res := range resCh {
		if res.Error != nil {
			result.Errors = append(result.Errors, ExportError{
				ObjectType: objectType,
				Error:      res.Error.Error(),
				Timestamp:  time.Now(),
			})
		} else {
			result.Success[objectType]++
		}

		s.Plugin.Base.Reporter.Report(map[string]interface{}{
			"objectType": objectType,
			"successful": result.Success,
			"errors":     len(result.Errors),
			"total":      len(objects),
		})
	}

	return result, nil
}

func (s *Service) getAllDatabases(ctx context.Context) ([]*types.Database, error) {
	searchCtx, cancel := context.WithTimeout(ctx, s.Plugin.Config.Timeout)
	defer cancel()

	ch := s.Plugin.NotionClient.Registry.Search().Stream(searchCtx, types.SearchRequest{
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
	result, err := client.ToGoResult(s.Plugin.NotionClient.Registry.Databases().Get(ctx, databaseID))
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Service) getPagesFromDatabase(ctx context.Context, databaseID types.DatabaseID) ([]*types.Page, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.Plugin.Config.Timeout)
	defer cancel()

	ch := s.Plugin.NotionClient.Registry.Databases().Query(queryCtx, databaseID, nil, nil)

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
	result := s.Plugin.NotionClient.Registry.Pages().Get(ctx, pageID)
	if result.IsError() {
		return nil, result.Error
	}
	return &result.Data, nil
}

func (s *Service) exportDatabases(ctx context.Context, databases []*types.Database) (*ExportResult, error) {
	objects := make([]interface{}, len(databases))
	for i, db := range databases {
		objects[i] = db
	}
	return s.schedule(ctx, objects, types.ObjectTypeDatabase)
}

func (s *Service) exportPages(ctx context.Context, pages []*types.Page) (*ExportResult, error) {
	objects := make([]interface{}, len(pages))
	for i, page := range pages {
		objects[i] = page
	}
	return s.schedule(ctx, objects, types.ObjectTypePage)
}

// exportPageBlocks exports all blocks from a specific page.
func (s *Service) exportPageBlocks(ctx context.Context, pageID types.PageID) (*ExportResult, error) {
	// Convert PageID to BlockID since pages can be treated as blocks for getting their children
	blockID := types.BlockID(pageID)

	// Get page blocks using the new GetChildren API
	ch := s.Plugin.NotionClient.Registry.Blocks().GetChildren(ctx, blockID)

	var blocks []*types.Block
	for result := range ch {
		if result.Error != nil {
			// Return error if we can't get blocks
			return NewExportResult(), result.Error
		}
		blocks = append(blocks, &result.Data)
	}

	// Export the blocks
	objects := make([]interface{}, len(blocks))
	for i, block := range blocks {
		objects[i] = block
	}

	return s.schedule(ctx, objects, types.ObjectTypeBlock)
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

// workResult represents the result of a worker operation.
type workResult struct {
	Key    string
	Object interface{}
	Error  error
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
	Start   time.Time                `json:"start"`
	End     time.Time                `json:"end"`
	Success map[types.ObjectType]int `json:"success"`
	Errors  []ExportError            `json:"errors,omitempty"`
	mu      sync.RWMutex
}

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

func (e *ExportResult) Successful(objectType types.ObjectType, r *ExportResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.Success[objectType] += r.Success[objectType]
	e.Errors = append(e.Errors, r.Errors...)
}
