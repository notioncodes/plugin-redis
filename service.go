// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/mateothegreat/go-multilog/multilog"

	"github.com/notioncodes/client"
	"github.com/notioncodes/types"
)

// Service provides high-level Redis export functionality with
// concurrent processing and batch operations for efficient Notion data export.
type Service struct {
	Plugin      *Plugin
	transformer *Transformer
	result      *ExportResult
	wg          sync.WaitGroup
	objCh       chan interface{}
	resCh       chan workResult
}

// NewService creates a new Redis service instance.
//
// Arguments:
// - config: The Redis service configuration.
//
// Returns:
// - The Redis service instance.
// - An error if the Redis service creation fails.
func NewService(plugin *Plugin) (*Service, error) {
	return &Service{
		Plugin:      plugin,
		transformer: NewTransformer(plugin),
		result:      NewExportResult(),
	}, nil
}

// Export exports all accessible Notion content to Redis.
//
// Arguments:
// - ctx: The context for the export operation.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) Export(ctx context.Context) (*ExportResult, error) {
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

	wg := sync.WaitGroup{}

	// Export pages from each database if enabled.
	if slices.Contains(s.Plugin.Config.Content.Types, types.ObjectTypePage) {
		for _, db := range databases {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pages, err := s.getPagesFromDatabase(ctx, db.ID)
				if err != nil {
					s.result.Errored(types.ObjectTypeDatabase, db.ID.String(), err)
					return
				}

				for _, page := range pages {
					data, err := s.transformer.Transform(ctx, types.ObjectTypePage, page)
					if err != nil {
						s.result.Errored(types.ObjectTypePage, page.ID.String(), err)
						return
					}

					err = s.Plugin.RedisClient.Request(StoreRequest{
						ObjectType: types.ObjectTypePage,
						Object:     data,
						TTL:        s.Plugin.Config.ClientConfig.TTL,
					})
					if err != nil {
						s.result.Errored(types.ObjectTypePage, page.ID.String(), err)
						if !s.Plugin.Config.Common.ContinueOnError {
							return
						}
					} else {
						// Count successful page exports
						s.result.mu.Lock()
						s.result.Success[types.ObjectTypePage]++
						s.result.mu.Unlock()
					}
				}

				// Export blocks from pages if enabled.
				if s.Plugin.Config.Content.Pages.Blocks && slices.Contains(s.Plugin.Config.Content.Types, types.ObjectTypeBlock) {
					for _, page := range pages {
						blockResult, err := s.exportPageBlocks(ctx, page.ID)
						if err != nil {
							s.result.Errored(types.ObjectTypeBlock, page.ID.String(), err)
							return
						}
						s.result.Successful(types.ObjectTypeBlock, blockResult)
					}
				}
			}()
		}
		wg.Wait()
	}

	// Wait for all Redis workers to finish processing queued items
	if s.Plugin.RedisClient != nil {
		err := s.Plugin.RedisClient.Close()
		if err != nil {
			multilog.Error("service", "failed to close redis client", map[string]interface{}{
				"error": err,
			})
		}
	}

	s.result.End = time.Now()

	fmt.Printf("exported %d pages\n", s.result.Success[types.ObjectTypePage])

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
	for i := 0; i < s.Plugin.Config.Common.Workers; i++ {
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

				err = s.Plugin.RedisClient.Request(StoreRequest{
					ObjectType: objectType,
					Object:     data,
					TTL:        s.Plugin.Config.ClientConfig.TTL,
				})
				if err != nil {
					resCh <- workResult{
						Object: obj,
						Error:  err,
					}
					continue
				}

				// Send success result
				resCh <- workResult{
					Object: obj,
					Error:  nil,
				}
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

		httpMetrics := s.Plugin.NotionClient.GetMetrics()
		s.Plugin.Base.Reporter.Report(map[string]interface{}{
			"objectType":               objectType,
			"successful":               result.Success,
			"errors":                   len(result.Errors),
			"total":                    result.Total(),
			"http_throttled":           httpMetrics.ThrottleWaits,
			"http_retries":             httpMetrics.TotalRetries,
			"http_errors":              httpMetrics.TotalErrors,
			"http_requests":            httpMetrics.TotalRequests,
			"http_requests_per_second": httpMetrics.RequestsPerSecond,
		})
	}

	return result, nil
}

func (s *Service) getAllDatabases(ctx context.Context) ([]*types.Database, error) {
	searchCtx := ctx
	var cancel context.CancelFunc
	if s.Plugin.Config.Common.RuntimeTimeout > 0 {
		searchCtx, cancel = context.WithTimeout(ctx, s.Plugin.Config.Common.RuntimeTimeout)
	}
	if cancel != nil {
		defer cancel()
	}

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
				return databases, fmt.Errorf("search timeout after %v", s.Plugin.Config.Common.RuntimeTimeout)
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
	queryCtx := ctx
	var cancel context.CancelFunc
	if s.Plugin.Config.Common.RuntimeTimeout > 0 {
		queryCtx, cancel = context.WithTimeout(ctx, s.Plugin.Config.Common.RuntimeTimeout)
	}
	if cancel != nil {
		defer cancel()
	}

	ch := s.Plugin.NotionClient.Registry.Databases().Query(queryCtx, databaseID, nil, nil)

	var pages []*types.Page
	for result := range ch {
		if result.Error != nil {
			if result.Error == context.DeadlineExceeded {
				return pages, fmt.Errorf("database query timeout after %v", s.Plugin.Config.Common.RuntimeTimeout)
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

// exportPageBlocks exports all blocks from a specific page.
func (s *Service) exportPageBlocks(ctx context.Context, pageID types.PageID) (*ExportResult, error) {
	ch := s.Plugin.NotionClient.Registry.Blocks().GetChildren(ctx, types.BlockID(pageID.String()))

	var blocks []*types.Block
	for result := range ch {
		if result.Error != nil {
			return NewExportResult(), result.Error
		}
		blocks = append(blocks, &result.Data)
	}

	objects := make([]interface{}, len(blocks))
	for i, block := range blocks {
		objects[i] = block
	}

	return s.schedule(ctx, objects, types.ObjectTypeBlock)
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

// Successful adds a successful export to the export result.
//
// Arguments:
// - objectType: The type of object that was exported.
// - r: The export result for the object.
func (e *ExportResult) Successful(objectType types.ObjectType, r *ExportResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.Success[objectType] += r.Success[objectType]
	e.Errors = append(e.Errors, r.Errors...)
}

// Total returns the total number of objects exported.
//
// Returns:
// - The total number of objects exported.
func (e *ExportResult) Total() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int
	for _, count := range e.Success {
		total += count
	}
	return total + len(e.Errors)
}
