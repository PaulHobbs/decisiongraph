package dag

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Value struct {
	NodeID string
	Data   interface{}
}

type Node struct {
	ID       string
	Requires []string
}

type Graph struct {
	Nodes map[string]*Node
}

type ExecuteNode func(context.Context, *Node, []*Value) (*Value, error)

type output struct {
	value *Value
	err   error
}

type edge struct {
	from string
	to   string
}

func NewGraph() *Graph {
	return &Graph{
		Nodes: make(map[string]*Node),
	}
}

func (g *Graph) AddNode(id string, requires []string) error {
	fmt.Printf("[DEBUG] Adding node %s with dependencies %v\n", id, requires)
	if _, exists := g.Nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}
	g.Nodes[id] = &Node{
		ID:       id,
		Requires: requires,
	}
	return nil
}

func (g *Graph) broadcastOutput(nodeID string, result output, edges map[edge]chan output) {
	fmt.Printf("[DEBUG] Broadcasting output from node %s (err: %v)\n", nodeID, result.err)
	for _, depNode := range g.Nodes {
		if e := (edge{from: nodeID, to: depNode.ID}); edges[e] != nil {
			fmt.Printf("[DEBUG] Sending output from %s to %s\n", nodeID, depNode.ID)
			edges[e] <- result
			fmt.Printf("[DEBUG] Sent output from %s to %s\n", nodeID, depNode.ID)
		}
	}
}

func (g *Graph) Run(ctx context.Context, executeNode ExecuteNode) error {
	fmt.Printf("[DEBUG] Starting graph execution at %v\n", time.Now())

	// Find leaf nodes (nodes that no other nodes depend on)
	leafNodes := make(map[string]bool)
	for id := range g.Nodes {
		leafNodes[id] = true
	}
	for _, node := range g.Nodes {
		for _, req := range node.Requires {
			delete(leafNodes, req)
		}
	}

	// Add FINISH node to the graph
	finishNode := &Node{
		ID:       "FINISH",
		Requires: make([]string, 0, len(leafNodes)),
	}
	for id := range leafNodes {
		finishNode.Requires = append(finishNode.Requires, id)
	}
	g.Nodes["FINISH"] = finishNode

	// Create channels for each edge in the graph
	edges := make(map[edge]chan output)
	for _, node := range g.Nodes {
		for _, reqID := range node.Requires {
			e := edge{from: reqID, to: node.ID}
			edges[e] = make(chan output, 1)
			fmt.Printf("[DEBUG] Created channel for edge %s -> %s\n", reqID, node.ID)
		}
	}

	// WaitGroup to track all node completions
	var wg sync.WaitGroup
	var leafErrors []error

	// Start each node in its own goroutine, including FINISH
	for _, node := range g.Nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()

			if n.ID == "FINISH" {
				// Handle FINISH node differently: collect results instead of executing
				fmt.Printf("[DEBUG] Starting FINISH node to collect results\n")
				for _, reqID := range n.Requires {
					e := edge{from: reqID, to: n.ID}
					select {
					case <-ctx.Done():
						fmt.Printf("[DEBUG] Context cancelled while FINISH node waiting for %s\n", reqID)
						return
					case result := <-edges[e]:
						if result.err != nil {
							leafErrors = append(leafErrors, fmt.Errorf("leaf node %s failed: %w", reqID, result.err))
						} else {
							fmt.Printf("[DEBUG] FINISH node received result from %s: %v\n", reqID, result.value)
						}
					}
				}
				if len(leafErrors) > 0 {
					fmt.Printf("[DEBUG] FINISH node detected errors: %v\n", leafErrors)
				} else {
					fmt.Printf("[DEBUG] FINISH node completed successfully\n")
				}
				return
			}

			// Regular node execution
			startTime := time.Now()
			fmt.Printf("[DEBUG] Starting execution of node %s at %v\n", n.ID, startTime)

			// ... (rest of the regular node execution logic)
			inputs := make([]*Value, 0, len(n.Requires))
			for _, reqID := range n.Requires {
				fmt.Printf("[DEBUG] Node %s waiting for input from %s\n", n.ID, reqID)
				e := edge{from: reqID, to: n.ID}
				select {
				case <-ctx.Done():
					fmt.Printf("[DEBUG] Context cancelled while node %s waiting for %s\n", n.ID, reqID)
					result := output{err: ctx.Err()}
					g.broadcastOutput(n.ID, result, edges)
					return
				case result := <-edges[e]:
					fmt.Printf("[DEBUG] Node %s received input from %s (err: %v)\n", n.ID, reqID, result.err)
					if result.err != nil {
						result = output{err: fmt.Errorf("dependency %s failed: %w", reqID, result.err)}
						g.broadcastOutput(n.ID, result, edges)
						return
					}
					inputs = append(inputs, result.value)
				}
			}

			fmt.Printf("[DEBUG] Node %s executing with %d inputs\n", n.ID, len(inputs))
			value, err := executeNode(ctx, n, inputs)
			result := output{value: value, err: err}
			fmt.Printf("[DEBUG] Node %s execution completed with err: %v\n", n.ID, err)
			g.broadcastOutput(n.ID, result, edges)

			fmt.Printf("[DEBUG] Completed execution of node %s, took %v\n", n.ID, time.Since(startTime))

		}(node)
	}

	// Wait for completion or context cancellation
	done := make(chan struct{})
	go func() {
		fmt.Printf("[DEBUG] Waiting for all nodes to complete\n")
		wg.Wait()
		fmt.Printf("[DEBUG] All nodes completed\n")
		close(done)
	}()

	select {
	case <-ctx.Done():
		fmt.Printf("[DEBUG] Context cancelled while waiting for completion at %v\n", time.Now())
		return ctx.Err()
	case <-done:
		fmt.Printf("[DEBUG] Graph execution completed at %v\n", time.Now())
	}

	fmt.Printf("[DEBUG] Graph execution completed successfully at %v\n", time.Now())
	if len(leafErrors) > 0 {
		return fmt.Errorf("leaf nodes got errors, including: %w", leafErrors[0])
	}
	return nil
}
