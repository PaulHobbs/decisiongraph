package dag

import (
	"context"
	"fmt"
)

type Middleware[T any, U any] func(ExecuteNode[T]) ExecuteNode[U]

// Cached is a ExecuteNode middleware for saving and loading node results. This can allow for resumable graphs by using external storage.
func Cached[T any](
    save func(string, []*Value[T], *Value[T]) error,
    load func(string) (*Value[T], error)) Middleware[T, T] {
    return func(runnode ExecuteNode[T]) ExecuteNode[T] {
        return func(ctx context.Context, node *Node, inputs []*Value[T]) (*Value[T], error) {
            // Attempt to load cached result for this node
            cachedValue, err := load(node.ID)
            if err == nil {
                // Cache hit: return the cached value
                return cachedValue, nil
            }

            // Cache miss: execute the actual node logic
            result, err := runnode(ctx, node, inputs)
            if err != nil {
                return nil, err // Propagate any errors from the node execution
            }

            // Save the result to cache for future executions
            if saveErr := save(node.ID, inputs, result); saveErr != nil {
                // Log or handle caching error (optional)
                fmt.Printf("Warning: failed to cache result for node %s: %v\n", node.ID, saveErr)
            }

            return result, nil
        }
    }
}
