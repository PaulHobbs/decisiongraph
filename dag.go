package dag

import (
	"context"
	"fmt"
	"sync"
)

// Input represents the result of executing a Node
type Input struct {
	NodeID string
	Data   interface{}
}

// Node represents a single node in the computation graph
type Node struct {
	ID       string
	Requires []string // IDs of nodes that must complete before this one
}

// Graph represents the computation DAG
type Graph struct {
	Nodes map[string]*Node
}

type ExecuteNode func(context.Context, *Node, []*Input) (*Input, error)

// NewGraph creates a new computation graph
func NewGraph() *Graph {
	return &Graph{
		Nodes: make(map[string]*Node),
	}
}

// AddNode adds a node to the graph
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

// RunContext holds context for running a node
type RunContext struct {
	Ctx         context.Context
	ExecuteNode ExecuteNode
	Results     map[string]*Input
	Errors      map[string]error
	Done        chan string
	Failed      chan string
	ResultsMu   *sync.RWMutex
	ErrorsMu    *sync.RWMutex
}

// runNode executes a single node
func (rc *RunContext) runNode(node *Node, inputs []*Input) {
	result, err := rc.ExecuteNode(rc.Ctx, node, inputs)
	if err != nil {
		rc.ErrorsMu.Lock()
		rc.Errors[node.ID] = err
		rc.ErrorsMu.Unlock()
		rc.Failed <- node.ID
		return
	}
	rc.ResultsMu.Lock()
	rc.Results[node.ID] = result
	rc.ResultsMu.Unlock()
	rc.Done <- node.ID
}

// startNodesWithNoDependencies starts nodes with no dependencies
func (rc *RunContext) startNodesWithNoDependencies(g *Graph) {
	for _, node := range g.Nodes {
		if len(node.Requires) == 0 {
			go rc.ExecuteNode(rc.Ctx, node, nil)
		}
	}
}

// canRun checks if a node can run
func (g *Graph) canRun(node *Node, ctx *RunContext) (bool, []*Input) {
	ctx.ResultsMu.RLock()
	defer ctx.ResultsMu.RUnlock()

	inputs := make([]*Input, 0, len(node.Requires))
	for _, reqID := range node.Requires {
		ctx.ErrorsMu.RLock()
		if _, exists := ctx.Errors[reqID]; exists {
			ctx.ErrorsMu.RUnlock()
			return false, nil
		}
		ctx.ErrorsMu.RUnlock()

		if result, exists := ctx.Results[reqID]; !exists {
			return false, nil
		} else {
			inputs = append(inputs, result)
		}
	}
	return true, inputs
}

// Run executes the graph in parallel
// runNode is the function that will be called to execute each node
func (g *Graph) Run(ctx context.Context, runNode ExecuteNode) error {
	runCtx := &RunContext{
		Ctx:         ctx,
		ExecuteNode: runNode,
		Results:     make(map[string]*Input),
		Errors:      make(map[string]error),
		Done:        make(chan string),
		Failed:      make(chan string),
		ResultsMu:   &sync.RWMutex{},
		ErrorsMu:    &sync.RWMutex{},
	}

	var managerWg sync.WaitGroup
	managerWg.Add(1)
	go func() {
		defer managerWg.Done()
		pending := make(map[string]bool)
		for id := range g.Nodes {
			pending[id] = true
		}

		for len(pending) > 0 {
			select {
			case <-ctx.Done():
				return
			case nodeID := <-runCtx.Done:
				delete(pending, nodeID)
				for id := range pending {
					if ready, inputs := g.canRun(g.Nodes[id], runCtx); ready {
						go runCtx.runNode(g.Nodes[id], inputs)
					}
				}
			case nodeID := <-runCtx.Failed:
				delete(pending, nodeID)
			}
		}
	}()

	runCtx.startNodesWithNoDependencies(g)

	for id := range g.Nodes {
		if _, ok := runCtx.Results[id]; !ok {
			if _, ok := runCtx.Errors[id]; ok {
				continue
			}
			if ready, inputs := g.canRun(g.Nodes[id], runCtx); ready {
				go runNode(runCtx.Ctx, g.Nodes[id], inputs)
			}
		}
	}

	for range g.Nodes {
		select {
		case <-runCtx.Done:
		case <-runCtx.Failed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	managerWg.Wait()

	if len(runCtx.Errors) > 0 {
		return fmt.Errorf("graph execution failed with %d errors", len(runCtx.Errors))
	}
	return nil
}
