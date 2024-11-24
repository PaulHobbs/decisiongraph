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

type nodeCtx struct {
	self        *Node
	ingress     map[edge]chan output
	egress      map[edge]chan output
	executeNode ExecuteNode
}

func newNodeCtx(self *Node, edges map[edge]chan output, executeNode ExecuteNode) nodeCtx {
	ingress := make(map[edge]chan output)
	egress := make(map[edge]chan output)
	for e, channel := range edges {
		if e.from == self.ID {
			egress[e] = channel
		} else if e.to == self.ID {
			ingress[e] = channel
		}
	}
	return nodeCtx{self: self, ingress: ingress, egress: egress, executeNode: executeNode}
}

func (nc *nodeCtx) broadcast(result output) {
	fmt.Printf("[DEBUG] Broadcasting output from node %s (err: %v)\n", nc.self.ID, result.err)
	for e, channel := range nc.egress {
		fmt.Printf("[DEBUG] Sending output from %s to %s\n", e.from, e.to)
		channel <- result
		fmt.Printf("[DEBUG] Sent output from %s to %s\n", e.from, e.to)
	}
}

func (nc *nodeCtx) runFinish(ctx context.Context) []error {
	// Handle FINISH node differently: collect results instead of executing
	fmt.Printf("[DEBUG] Starting FINISH node to collect results\n")
	var leafErrors []error
	for e, channel := range nc.ingress {
		select {
		case <-ctx.Done():
			fmt.Printf("[DEBUG] Context cancelled while FINISH node waiting for %s\n", e.from)
			return leafErrors
		case result := <-channel:
			if result.err != nil {
				leafErrors = append(leafErrors, fmt.Errorf("leaf node %s failed: %w", e.from, result.err))
			} else {
				fmt.Printf("[DEBUG] FINISH node received result from %s: %v\n", e.from, result.value)
			}
		}
	}
	if len(leafErrors) > 0 {
		fmt.Printf("[DEBUG] FINISH node detected errors: %v\n", leafErrors)
	} else {
		fmt.Printf("[DEBUG] FINISH node completed successfully\n")
	}
	return leafErrors

}

func (nc *nodeCtx) runNode(ctx context.Context) {
	n := nc.self
	startTime := time.Now()
	fmt.Printf("[DEBUG] Starting execution of node %s at %v\n", n.ID, startTime)

	inputs := make([]*Value, 0, len(n.Requires))
	for e, channel := range nc.ingress {
		fmt.Printf("[DEBUG] Node %s waiting for input from %s\n", n.ID, e.from)
		select {
		case <-ctx.Done():
			fmt.Printf("[DEBUG] Context cancelled while node %s waiting for %s\n", n.ID, e.from)
			result := output{err: ctx.Err()}
			nc.broadcast(result)
			return
		case result := <-channel:
			fmt.Printf("[DEBUG] Node %s received input from %s (err: %v)\n", n.ID, e.from, result.err)
			if result.err != nil {
				result = output{err: fmt.Errorf("dependency %s failed: %w", e.from, result.err)}
				nc.broadcast(result)
				return
			}
			inputs = append(inputs, result.value)
		}
	}

	fmt.Printf("[DEBUG] Node %s executing with %d inputs\n", n.ID, len(inputs))
	value, err := nc.executeNode(ctx, n, inputs)
	result := output{value: value, err: err}
	fmt.Printf("[DEBUG] Node %s execution completed with err: %v\n", n.ID, err)
	nc.broadcast(result)

	fmt.Printf("[DEBUG] Completed execution of node %s, took %v\n", n.ID, time.Since(startTime))
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
		go func(self *Node) {
			defer wg.Done()
			nc := newNodeCtx(self, edges, executeNode)
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
