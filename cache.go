package dag

func cached[T any](
    save func(string, *Value[T]) error,
    load func(string) (*Value[T], error),
    runnode ExecuteNode[T],
) ExecuteNode[T] {
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
        if saveErr := save(node.ID, result); saveErr != nil {
            // Log or handle caching error (optional)
            fmt.Printf("Warning: failed to cache result for node %s: %v\n", node.ID, saveErr)
        }

        return result, nil
    }
}
