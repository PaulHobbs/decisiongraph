package dag

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestNodeExecutionCount(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B1", []string{"A"})
	g.AddNode("B2", []string{"A"})
	g.AddNode("B3", []string{"A"})

	executionCounts := make(map[string]int)
	var mu sync.Mutex

	err := g.Run(context.Background(), func(ctx context.Context, node *Node, inputs []*Value) (*Value, error) {
		mu.Lock()
		executionCounts[node.ID]++
		mu.Unlock()
		return &Value{NodeID: node.ID, Data: "result"}, nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	for _, id := range []string{"A", "B1", "B2", "B3"} {
		if count := executionCounts[id]; count != 1 {
			t.Errorf("Node %s executed %d times, expected 1", id, count)
		}
	}
}

func TestParallelExecution(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B1", []string{"A"})
	g.AddNode("B2", []string{"A"})
	g.AddNode("C", []string{"B1", "B2"})

	var (
		mu             sync.Mutex
		executionTimes = make(map[string]time.Time)
	)

	err := g.Run(context.Background(), func(ctx context.Context, node *Node, inputs []*Value) (*Value, error) {
		mu.Lock()
		executionTimes[node.ID] = time.Now()
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return &Value{NodeID: node.ID, Data: "result"}, nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Check B1 and B2 ran in parallel
	b1Time := executionTimes["B1"]
	b2Time := executionTimes["B2"]
	if diff := b1Time.Sub(b2Time).Abs(); diff > 50*time.Millisecond {
		t.Errorf("B1 and B2 did not run in parallel, time difference: %v", diff)
	}

	// Check C ran after both B1 and B2
	cTime := executionTimes["C"]
	if cTime.Before(b1Time) || cTime.Before(b2Time) {
		t.Error("C ran before its dependencies completed")
	}
}

func TestErrorPropagation(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B", []string{"A"})
	g.AddNode("C", []string{"B"})

	expectedErr := errors.New("node B failed")

	executed := make(map[string]bool)
	var mu sync.Mutex

	err := g.Run(context.Background(), func(ctx context.Context, node *Node, inputs []*Value) (*Value, error) {
		mu.Lock()
		executed[node.ID] = true
		mu.Unlock()

		if node.ID == "B" {
			return nil, expectedErr
		}
		return &Value{NodeID: node.ID, Data: "result"}, nil
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !executed["A"] {
		t.Error("Node A should have executed")
	}
	if !executed["B"] {
		t.Error("Node B should have executed")
	}
	if executed["C"] {
		t.Error("Node C should not have executed")
	}
}

func TestContextCancellation(t *testing.T) {
	g := NewGraph()
	g.AddNode("A", nil)
	g.AddNode("B", []string{"A"})

	ctx, cancel := context.WithCancel(context.Background())

	executed := make(map[string]bool)
	var mu sync.Mutex

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := g.Run(ctx, func(ctx context.Context, node *Node, inputs []*Value) (*Value, error) {
		mu.Lock()
		executed[node.ID] = true
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return &Value{NodeID: node.ID, Data: "result"}, nil
	})

	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	if len(executed) == len(g.Nodes) {
		t.Error("all nodes executed despite context cancellation")
	}
}

func generateRandomDAG(numNodes int, seed int64) (*Graph, error) {
	gen := rand.New(rand.NewSource(seed))
	g := NewGraph()
	fmt.Printf("numNodes: %d\n", numNodes)
	nodes := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = fmt.Sprintf("Node%d", i)
	}

	for i := 0; i < int(numNodes); i++ {
		var numDependencies int
		if i == 0 {
			numDependencies = 0
		} else {
			numDependencies = gen.Intn(i) // A node can only depend on nodes with smaller index to avoid cycles.
		}

		dependencies := make([]string, 0)
		for j := 0; j < numDependencies; j++ {
			dependencyIndex := gen.Intn(i)
			dependencies = append(dependencies, nodes[dependencyIndex])
		}
		err := g.AddNode(nodes[i], dependencies)
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

func FuzzDAGExecution(f *testing.F) {
	for numNodes := uint(0); numNodes < 100; numNodes++ {
		f.Add(numNodes, int64(42)) // Add an example seed corpus value.
	}

	f.Fuzz(func(t *testing.T, numNodes uint, seed int64) {
		numNodes = (numNodes % 100) + 1
		g, err := generateRandomDAG(int(numNodes), seed)
		if err != nil {
			t.Fatalf("failed to generate random DAG: %v", err)
		}

		err = g.Run(context.Background(), func(ctx context.Context, node *Node, inputs []*Value) (*Value, error) {
			return &Value{NodeID: node.ID, Data: "result"}, nil
		})
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	})
}
