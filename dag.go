package dag

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Value[T any] struct {
	NodeID string
	Data   T
}

type Node struct {
	ID       string
	Requires []string
}

type Graph[T any] struct {
	Nodes   map[string]*Node
	LogFunc func(format string, v ...any) // Injectable logging function
}

type ExecuteNode[T any] func(context.Context, *Node, []*Value[T]) (*Value[T], error)

type output[T any] struct {
	value *Value[T]
	err   error
}

type edge struct {
	from string
	to   string
}

func NewGraph[T any](opts ...func(*Graph[T])) *Graph[T] {
	g := &Graph[T]{
		Nodes: make(map[string]*Node),
		LogFunc: func(format string, v ...any) {
			// Default: no logging
		},
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

func (g *Graph[T]) AddNode(id string, requires []string) error {
	g.LogFunc("[DEBUG] Adding node %s with dependencies %v\n", id, requires)
	if _, exists := g.Nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}
	g.Nodes[id] = &Node{
		ID:       id,
		Requires: requires,
	}
	return nil
}

func WithLogging[T any](logFunc func(format string, v ...any)) func(*Graph[T]) {
	return func(g *Graph[T]) {
		g.LogFunc = logFunc
	}
}

type nodeCtx[T any] struct {
	self        *Node
	ingress     map[edge]chan output[T]
	egress      map[edge]chan output[T]
	executeNode ExecuteNode[T]
	logFunc     func(format string, v ...any)
}

func newNodeCtx[T any](self *Node, edges map[edge]chan output[T], executeNode ExecuteNode[T], logFunc func(format string, v ...any)) nodeCtx[T] {
	ingress := make(map[edge]chan output[T])
	egress := make(map[edge]chan output[T])
	for e, channel := range edges {
		if e.from == self.ID {
			egress[e] = channel
		} else if e.to == self.ID {
			ingress[e] = channel
		}
	}
	return nodeCtx[T]{self: self, ingress: ingress, egress: egress, executeNode: executeNode, logFunc: logFunc}
}

func (nc *nodeCtx[T]) broadcast(result output[T]) {
	nc.logFunc("[DEBUG] Broadcasting output from node %s (err: %v)\n", nc.self.ID, result.err)
	for e, channel := range nc.egress {
		nc.logFunc("[DEBUG] Sending output from %s to %s\n", e.from, e.to)
		channel <- result
		nc.logFunc("[DEBUG] Sent output from %s to %s\n", e.from, e.to)
	}
}

func (nc *nodeCtx[T]) runFinish(ctx context.Context) []error {
	nc.logFunc("[DEBUG] Starting FINISH node to collect results\n")
	var leafErrors []error
	for e, channel := range nc.ingress {
		select {
		case <-ctx.Done():
			nc.logFunc("[DEBUG] Context cancelled while FINISH node waiting for %s\n", e.from)
			return leafErrors
		case result := <-channel:
			if result.err != nil {
				leafErrors = append(leafErrors, fmt.Errorf("leaf node %s failed: %w", e.from, result.err))
			} else {
				nc.logFunc("[DEBUG] FINISH node received result from %s: %v\n", e.from, result.value)
			}
		}
	}
	if len(leafErrors) > 0 {
		nc.logFunc("[DEBUG] FINISH node detected errors: %v\n", leafErrors)
	} else {
		nc.logFunc("[DEBUG] FINISH node completed successfully\n")
	}
	return leafErrors

}

func (nc *nodeCtx[T]) runNode(ctx context.Context) {
	n := nc.self
	startTime := time.Now()
	nc.logFunc("[DEBUG] Starting execution of node %s at %v\n", n.ID, startTime)

	inputs := make([]*Value[T], 0, len(n.Requires))
	for e, channel := range nc.ingress {
		nc.logFunc("[DEBUG] Node %s waiting for input from %s\n", n.ID, e.from)
		select {
		case <-ctx.Done():
			nc.logFunc("[DEBUG] Context cancelled while node %s waiting for %s\n", n.ID, e.from)
			result := output[T]{err: ctx.Err()}
			nc.broadcast(result)
			return
		case result := <-channel:
			nc.logFunc("[DEBUG] Node %s received input from %s (err: %v)\n", n.ID, e.from, result.err)
			if result.err != nil {
				result = output[T]{err: fmt.Errorf("dependency %s failed: %w", e.from, result.err)}
				nc.broadcast(result)
				return
			}
			inputs = append(inputs, result.value)
		}
	}

	nc.logFunc("[DEBUG] Node %s executing with %d inputs\n", n.ID, len(inputs))
	value, err := nc.executeNode(ctx, n, inputs)
	result := output[T]{value: value, err: err}
	nc.logFunc("[DEBUG] Node %s execution completed with err: %v\n", n.ID, err)
	nc.broadcast(result)

	nc.logFunc("[DEBUG] Completed execution of node %s, took %v\n", n.ID, time.Since(startTime))
}

func (g *Graph[T]) Run(ctx context.Context, executeNode ExecuteNode[T]) error {
	g.LogFunc("[DEBUG] Starting graph execution at %v\n", time.Now())

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

	// Add FINISH node to the graph. This allows collecting the output of all the leaf nodes.
	finishNode := &Node{
		ID:       "FINISH",
		Requires: make([]string, 0, len(leafNodes)),
	}
	for id := range leafNodes {
		finishNode.Requires = append(finishNode.Requires, id)
	}
	g.Nodes["FINISH"] = finishNode

	// Create channels for each edge in the graph
	edges := make(map[edge]chan output[T])
	for _, node := range g.Nodes {
		for _, reqID := range node.Requires {
			e := edge{from: reqID, to: node.ID}
			edges[e] = make(chan output[T], 1)
			g.LogFunc("[DEBUG] Created channel for edge %s -> %s\n", reqID, node.ID)
		}
	}

	// WaitGroup to track all node completions
	var wg sync.WaitGroup
	var leafErrors []error

	// Start each node in its own goroutine, including FINISH
	for _, node := range g.Nodes {
		wg.Add(1)
		go func(self *Node) {
			defer wg.Done()
			nc := newNodeCtx(self, edges, executeNode, g.LogFunc)
			if self.ID == "FINISH" {
				leafErrors = nc.runFinish(ctx)
				return
			}
			nc.runNode(ctx)
		}(node)
	}

	// Wait for completion or context cancellation
	done := make(chan struct{})
	go func() {
		g.LogFunc("[DEBUG] Waiting for all nodes to complete\n")
		wg.Wait()
		g.LogFunc("[DEBUG] All nodes completed\n")
		close(done)
	}()

	select {
	case <-ctx.Done():
		g.LogFunc("[DEBUG] Context cancelled while waiting for completion at %v\n", time.Now())
		return ctx.Err()
	case <-done:
		g.LogFunc("[DEBUG] Graph execution completed at %v\n", time.Now())
	}

	g.LogFunc("[DEBUG] Graph execution completed successfully at %v\n", time.Now())
	if len(leafErrors) > 0 {
		return fmt.Errorf("leaf nodes got errors, including: %w", leafErrors[0])
	}
	return nil
}
