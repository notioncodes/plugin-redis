package redis

import "github.com/notioncodes/types"

// Helper function to check if a slice contains a specific object type.
func contains(slice []types.ObjectType, item types.ObjectType) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
