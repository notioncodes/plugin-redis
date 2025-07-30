// Package redis implements export and import functionality between the
// github.com/notioncodes/client package and redis servers.
package redis

import (
	"context"

	"github.com/notioncodes/types"
)

// Transformer is responsible for transforming the object into a format that
// can be stored in Redis.
type Transformer struct {
	plugin *Pugin
}

// NewTransformer creates a new Transformer instance.
//
// Arguments:
// - plugin: The plugin instance.
//
// Returns:
// - The Transformer instance.
func NewTransformer(plugin *Pugin) *Transformer {
	return &Transformer{
		plugin: plugin,
	}
}

// Transform processes a Notion object and stores it in Redis.
//
// Arguments:
// - ctx: The context for the export operation.
// - objectType: The type of object to export.
// - object: The object to export.
//
// Returns:
// - The transformed object as a JSON string byte slice.
// - An error if the export operation fails.
func (t *Transformer) Transform(ctx context.Context, objectType types.ObjectType, object interface{}) (interface{}, error) {
	// if t.plugin.Config.ClientConfig.IncludeMeta {
	// 	objWithMeta, err := common.MergeObjects(common.ToInterfaceMap(object), map[string]any{
	// 		"metadata": map[string]any{
	// 			"object_type": string(objectType),
	// 			"exported_at": time.Now(),
	// 		},
	// 	})
	// 	if err != nil {
	// 		return nil, fmt.Errorf("injecting metadata failed: %w", err)
	// 	}
	// 	object = objWithMeta
	// }

	return object, nil
}

// Cleanup closes the Redis connection.
func (t *Transformer) Cleanup() error {
	return nil
}
