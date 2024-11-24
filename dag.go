package dag

import (
	"context"
	"fmt"
	"sync"
)

type Input struct {
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

type ExecuteNode func(context.Context, *Node, []*Input) (*Input, error)

type nodeResult struct {
	input *Input
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
	if _, exists := g.Nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}
	g.Nodes[id] = &Node{
		ID:       id,
		Requires: requires,
	}
	return nil
}

func (g *Graph) sendResultToDependents(nodeID string, result nodeResult, edges map[edge]chan nodeResult) {
	for _, depNode := range g.Nodes {
		if e := (edge{from: nodeID, to: depNode.ID}); edges[e] != nil {
			edges[e] <- result
		}
	}
}

func (g *Graph) Run(ctx context.Context, executeNode ExecuteNode) error {
	// Create channels for each edge in the graph
	edges := make(map[edge]chan nodeResult)
	for _, node := range g.Nodes {
		for _, reqID := range node.Requires {
			edges[edge{from: reqID, to: node.ID}] = make(chan nodeResult, 1)
		}
	}

	// WaitGroup to track all node completions
	var wg sync.WaitGroup

	// Start each node in its own goroutine
	for _, node := range g.Nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()

			// Collect inputs from required nodes
			inputs := make([]*Input, 0, len(n.Requires))
			for _, reqID := range n.Requires {
				e := edge{from: reqID, to: n.ID}
				select {
				case <-ctx.Done():
					// Propagate cancellation to all dependent nodes
					g.sendResultToDependents(n.ID, nodeResult{err: ctx.Err()}, edges)
					return
				case result := <-edges[e]:
					if result.err != nil {
						// Propagate error to all dependent nodes
						g.sendResultToDependents(n.ID, nodeResult{err: fmt.Errorf("dependency %s failed: %w", reqID, result.err)}, edges)
						return
					}
					inputs = append(inputs, result.input)
				}
			}

			// Execute node
			input, err := executeNode(ctx, n, inputs)

			// Send result to all dependent nodes
			g.sendResultToDependents(n.ID, nodeResult{input: input, err: err}, edges)
		}(node)
	}

	// Wait for completion or context cancellation
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	// Check for errors in leaf nodes (nodes with no dependents)
	for id, node := range g.Nodes {
		isLeaf := true
		for _, otherNode := range g.Nodes {
			for _, req := range otherNode.Requires {
				if req == id {
					isLeaf = false
					break
				}
			}
			if !isLeaf {
				break
			}
		}

		if isLeaf {
			for _, reqID := range node.Requires {
				e := edge{from: reqID, to: node.ID}
				result := <-edges[e]
				if result.err != nil {
					return fmt.Errorf("node %s failed: %w", id, result.err)
				}
			}
		}
	}

	return nil
}
