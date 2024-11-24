package dag

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewGraph(t *testing.T) {
	g := NewGraph()
	if g == nil {
		t.Fatal("NewGraph() returned nil")
	}
	if g.Nodes == nil {
		t.Fatal("Nodes map not initialized")
	}
}

func TestAddNode(t *testing.T) {
	g := NewGraph()

	tests := []struct {
		name     string
		id       string
		requires []string
		wantErr  bool
	}{
		{
			name:     "add node without dependencies",
			id:       "A",
			requires: nil,
			wantErr:  false,
		},
		{
			name:     "add node with dependencies",
			id:       "B",
			requires: []string{"A"},
			wantErr:  false,
		},
		{
			name:     "duplicate node",
			id:       "A",
			requires: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := g.AddNode(tt.id, tt.requires)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddNode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				node, exists := g.Nodes[tt.id]
				if !exists {
					t.Fatal("Node not added to graph")
				}
				if node.ID != tt.id {
					t.Errorf("Node ID = %v, want %v", node.ID, tt.id)
				}
				if !stringSliceEqual(node.Requires, tt.requires) {
					t.Errorf("Node requires = %v, want %v", node.Requires, tt.requires)
				}
			}
		})
	}
}

func TestRunLinearGraph(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B", []string{"A"})
	g.AddNode("C", []string{"B"})

	executionOrder := make([]string, 0)
	var mu sync.Mutex

	err := g.Run(context.Background(), func(node *Node, inputs []*Input) (*Input, error) {
		mu.Lock()
		executionOrder = append(executionOrder, node.ID)
		mu.Unlock()
		return &Input{NodeID: node.ID, Data: "result"}, nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !isValidOrder(executionOrder, g) {
		t.Errorf("Invalid execution order: %v", executionOrder)
	}
}

func TestRunParallelGraph(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B1", []string{"A"})
	g.AddNode("B2", []string{"A"})
	g.AddNode("C", []string{"B1", "B2"})

	var wg sync.WaitGroup
	b1Done := make(chan struct{})
	b2Done := make(chan struct{})

	err := g.Run(context.Background(), func(node *Node, inputs []*Input) (*Input, error) {
		switch node.ID {
		case "B1":
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(b1Done)
				time.Sleep(10 * time.Millisecond) // Simulate work

			}()
			return &Input{NodeID: node.ID, Data: "result"}, nil

		case "B2":
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(b2Done)
				time.Sleep(10 * time.Millisecond) // Simulate work
			}()
			return &Input{NodeID: node.ID, Data: "result"}, nil
		case "C":
			<-b1Done
			<-b2Done
			wg.Wait()

			return &Input{NodeID: node.ID, Data: "result"}, nil

		default:

			return &Input{NodeID: node.ID, Data: "result"}, nil
		}
	})

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

}

func TestRunWithErrors(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B", []string{"A"})
	g.AddNode("C", []string{"A"})

	expectedError := errors.New("node B failed")

	completed := make(map[string]bool)
	var mu sync.Mutex

	err := g.Run(context.Background(), func(node *Node, inputs []*Input) (*Input, error) {
		if node.ID == "B" {
			return nil, expectedError
		}
		mu.Lock()
		completed[node.ID] = true
		mu.Unlock()
		return &Input{NodeID: node.ID, Data: "result"}, nil
	})

	if err == nil {
		t.Fatal("Run() did not return error")
	}

	mu.Lock()
	if !completed["A"] {
		t.Error("Node A should have completed")
	}
	if completed["C"] {
		t.Error("Node C should not have completed due to error propagation")
	}
	mu.Unlock()
}

func TestRunWithContext(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B", []string{"A"})

	ctx, cancel := context.WithCancel(context.Background())

	var (
		mu        sync.Mutex
		completed = make(map[string]bool)
	)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := g.Run(ctx, func(node *Node, inputs []*Input) (*Input, error) {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		completed[node.ID] = true
		mu.Unlock()
		return &Input{NodeID: node.ID, Data: "result"}, nil
	})

	if err == nil {
		t.Fatal("Run() did not return error after context cancellation")
	}

	mu.Lock()
	completedCount := len(completed)
	mu.Unlock()
	if completedCount == len(g.Nodes) {
		t.Error("All nodes completed despite context cancellation")
	}
}

// Helper functions

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	copy(aCopy, a)
	copy(bCopy, b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	return strings.Join(aCopy, ",") == strings.Join(bCopy, ",")
}

func isValidOrder(order []string, g *Graph) bool {
	executed := make(map[string]bool)
	for _, nodeID := range order {
		node := g.Nodes[nodeID]
		for _, req := range node.Requires {
			if !executed[req] {
				return false
			}
		}
		executed[nodeID] = true
	}
	return len(order) == len(g.Nodes)
}

func TestNodeExecutionCount(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B1", []string{"A"})
	g.AddNode("B2", []string{"A"})
	g.AddNode("B3", []string{"A"})

	executionCounts := make(map[string]int)
	var mu sync.Mutex

	err := g.Run(context.Background(), func(ctx context.Context, node *Node, inputs []*Input) (*Input, error) {
		mu.Lock()
		executionCounts[node.ID]++
		mu.Unlock()
		return &Input{NodeID: node.ID, Data: "result"}, nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if executionCounts["A"] != 1 {
		t.Errorf("Node A executed %d times, expected 1", executionCounts["A"])
	}
	if executionCounts["B1"] != 1 {
		t.Errorf("Node B1 executed %d times, expected 1", executionCounts["B"])
	}
}
