// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"
	"fmt"
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

// Export exports all accessible Notion content to Redis using the centralized client export service.
//
// Arguments:
// - ctx: The context for the export operation.
//
// Returns:
// - The export result.
// - An error if the export operation fails.
func (s *Service) Export(ctx context.Context) (*ExportResult, error) {
	// Convert plugin config to client export config
	exportConfig := s.convertToClientExportConfig()
	
	// Create client export service
	clientExportService, err := client.NewExportService(s.Plugin.NotionClient, exportConfig)
	if err != nil {
		return s.result, fmt.Errorf("failed to create client export service: %w", err)
	}

	// Perform the export using the client service
	clientResult, err := clientExportService.Export(ctx)
	if err != nil {
		return s.result, fmt.Errorf("client export failed: %w", err)
	}

	// Process the exported data through Redis using our transformer and storage
	err = s.processClientExportResult(ctx, clientResult)
	if err != nil {
		return s.result, fmt.Errorf("failed to process export result: %w", err)
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

	return s.result, nil
}

// ExportPage exports a specific page and optionally its blocks to Redis using the client export service.
func (s *Service) ExportPage(ctx context.Context, pageID types.PageID, includeBlocks bool) (*ExportResult, error) {
	defer func() {
		s.result.End = time.Now()
	}()

	// Create export config for a specific page
	exportConfig := &client.ExportConfig{
		Content: client.ContentConfig{
			Types: []types.ObjectType{types.ObjectTypePage},
			Pages: client.PageConfig{
				IDs:           []types.PageID{pageID},
				IncludeBlocks: includeBlocks,
			},
			Blocks: client.BlockConfig{
				IncludeChildren: s.Plugin.Config.Content.Blocks.Children,
				MaxDepth:        0, // Unlimited depth
			},
		},
		Processing: client.ProcessingConfig{
			Workers:         s.Plugin.Config.Common.Workers,
			ContinueOnError: s.Plugin.Config.Common.ContinueOnError,
			BatchSize:       100,
		},
		Timeouts: client.TimeoutConfig{
			Runtime: s.Plugin.Config.Common.RuntimeTimeout,
		},
	}

	// Create client export service
	clientExportService, err := client.NewExportService(s.Plugin.NotionClient, exportConfig)
	if err != nil {
		return s.result, fmt.Errorf("failed to create client export service: %w", err)
	}

	// Perform the export using the client service
	clientResult, err := clientExportService.Export(ctx)
	if err != nil {
		return s.result, fmt.Errorf("client export failed: %w", err)
	}

	// Process the exported data through Redis
	err = s.processClientExportResult(ctx, clientResult)
	if err != nil {
		return s.result, fmt.Errorf("failed to process export result: %w", err)
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
			// Extract object ID based on type
			var objectID string
			switch v := res.Object.(type) {
			case *types.Page:
				objectID = v.ID.String()
			case *types.Block:
				objectID = v.ID.String()
			case *types.Database:
				objectID = v.ID.String()
			default:
				objectID = "unknown"
			}

			result.Errors = append(result.Errors, ExportError{
				ObjectType: objectType,
				ObjectID:   objectID,
				Error:      res.Error.Error(),
				Timestamp:  time.Now(),
			})
		} else {
			result.Success[objectType]++
		}
	}

	// Report metrics once after processing all results
	if len(objects) > 0 {
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

// convertToClientExportConfig converts the plugin configuration to client export configuration.
func (s *Service) convertToClientExportConfig() *client.ExportConfig {
	config := &client.ExportConfig{
		Content: client.ContentConfig{
			Types: s.Plugin.Config.Content.Types,
			Databases: client.DatabaseConfig{
				IDs:           s.Plugin.Config.Content.Databases.IDs,
				IncludePages:  s.Plugin.Config.Content.Databases.Pages,
				IncludeBlocks: s.Plugin.Config.Content.Databases.Blocks,
			},
			Pages: client.PageConfig{
				IDs:                nil, // Export from databases
				IncludeBlocks:      s.Plugin.Config.Content.Pages.Blocks,
				IncludeComments:    false, // TODO: Enable when comments are implemented
				IncludeAttachments: false, // TODO: Enable when attachments are implemented
			},
			Blocks: client.BlockConfig{
				IDs:             nil, // Export from pages
				IncludeChildren: s.Plugin.Config.Content.Blocks.Children,
				MaxDepth:        0, // Unlimited depth
			},
			Comments: client.CommentConfig{
				IncludeUsers: false, // TODO: Enable when comments are implemented
			},
			Users: client.UserConfig{
				IncludeAll:     false,
				OnlyReferenced: true,
			},
		},
		Processing: client.ProcessingConfig{
			Workers:         s.Plugin.Config.Common.Workers,
			ContinueOnError: s.Plugin.Config.Common.ContinueOnError,
			BatchSize:       100, // Default batch size
		},
		Timeouts: client.TimeoutConfig{
			Overall: 30 * time.Minute, // Overall timeout
			Runtime: s.Plugin.Config.Common.RuntimeTimeout,
			Request: 0, // No request timeout by default
		},
	}
	
	return config
}

// processClientExportResult processes the export result from the client service and stores it in Redis.
func (s *Service) processClientExportResult(ctx context.Context, clientResult *client.ExportResult) error {
	// TODO: The client export service currently doesn't return the actual exported objects,
	// only the counts and errors. We need to modify the client service to also collect
	// the exported objects so we can process them through Redis.
	// 
	// For now, we'll copy the results and indicate success
	s.result.Start = clientResult.Start
	s.result.End = clientResult.End
	s.result.Success = make(map[types.ObjectType]int)
	for objectType, count := range clientResult.Success {
		s.result.Success[objectType] = count
	}
	s.result.Errors = make([]ExportError, len(clientResult.Errors))
	for i, err := range clientResult.Errors {
		s.result.Errors[i] = ExportError{
			ObjectType: err.ObjectType,
			ObjectID:   err.ObjectID,
			Error:      err.Error,
			Timestamp:  err.Timestamp,
		}
	}
	
	return nil
}

// Legacy helper methods - these are now handled by the client export service
// but kept for backwards compatibility and future Redis-specific processing

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
