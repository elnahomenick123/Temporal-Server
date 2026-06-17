package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// WorkflowState represents the state of a workflow execution.
type WorkflowState int

const (
	WorkflowStateRunning WorkflowState = iota
	WorkflowStateCompleted
)

// ErrWorkflowContinuedAsNew is a retryable error indicating the workflow has continued as new.
type ErrWorkflowContinuedAsNew struct {
	NewRunID string
}

func (e *ErrWorkflowContinuedAsNew) Error() string {
	return fmt.Sprintf("workflow continued as new, new run ID: %s", e.NewRunID)
}

// MutableState represents the mutable state of a workflow run.
type MutableState struct {
	State    WorkflowState
	NewRunID string
	Value    string
}

// WorkflowExecutionContext manages access to a workflow run's state.
type WorkflowExecutionContext struct {
	sync.RWMutex
	mutableState *MutableState
}

// HistoryEngine simulates the history service.
type HistoryEngine struct {
	sync.RWMutex
	runs map[string]*WorkflowExecutionContext
}

func NewHistoryEngine() *HistoryEngine {
	return &HistoryEngine{
		runs: make(map[string]*WorkflowExecutionContext),
	}
}

func (e *HistoryEngine) CreateRun(runID string, value string) {
	e.Lock()
	defer e.Unlock()
	e.runs[runID] = &WorkflowExecutionContext{
		mutableState: &MutableState{
			State: WorkflowStateRunning,
			Value: value,
		},
	}
}

func (e *HistoryEngine) QueryWorkflow(ctx context.Context, runID string) (string, error) {
	e.RLock()
	context, exists := e.runs[runID]
	e.RUnlock()

	if !exists {
		return "", errors.New("run not found")
	}

	// Lock the context to prevent concurrent reads during state transitions
	context.RLock()
	defer context.RUnlock()

	state := context.mutableState
	if state.State == WorkflowStateCompleted && state.NewRunID != "" {
		// Reject query with retryable error
		return "", &ErrWorkflowContinuedAsNew{NewRunID: state.NewRunID}
	}

	// Simulate some query processing time
	time.Sleep(10 * time.Millisecond)

	return state.Value, nil
}

func (e *HistoryEngine) ContinueAsNew(runID string, newRunID string, newValue string) error {
	e.Lock()
	context, exists := e.runs[runID]
	e.Unlock()

	if !exists {
		return errors.New("run not found")
	}

	// Lock the context to commit the transaction
	context.Lock()
	context.mutableState.State = WorkflowStateCompleted
	context.mutableState.NewRunID = newRunID
	context.Unlock()

	// Create the new run
	e.CreateRun(newRunID, newValue)
	return nil
}

// Frontend simulates the frontend/matching service routing queries.
type Frontend struct {
	engine       *HistoryEngine
	currentRunID string
	mu           sync.RWMutex
}

func NewFrontend(engine *HistoryEngine, initialRunID string) *Frontend {
	return &Frontend{
		engine:       engine,
		currentRunID: initialRunID,
	}
}

func (f *Frontend) Query(ctx context.Context) (string, error) {
	for {
		f.mu.RLock()
		runID := f.currentRunID
		f.mu.RUnlock()

		val, err := f.engine.QueryWorkflow(ctx, runID)
		if err != nil {
			var continuedErr *ErrWorkflowContinuedAsNew
			if errors.As(err, &continuedErr) {
				// Invalidate cache and update current run ID
				f.mu.Lock()
				if f.currentRunID == runID {
					f.currentRunID = continuedErr.NewRunID
				}
				f.mu.Unlock()
				// Retry query on the new run ID
				continue
			}
			return "", err
		}
		return val, nil
	}
}

func main() {
	engine := NewHistoryEngine()
	initialRunID := "run-1"
	engine.CreateRun(initialRunID, "state-1")

	frontend := NewFrontend(engine, initialRunID)

	var wg sync.WaitGroup
	var querySuccessCount int64
	var queryStaleCount int64
	var queryNewStateCount int64

	// Concurrently query the workflow
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			val, err := frontend.Query(ctx)
			if err != nil {
				fmt.Printf("Query failed: %v\n", err)
				return
			}
			atomic.AddInt64(&querySuccessCount, 1)
			if val == "state-1" {
				atomic.AddInt64(&queryStaleCount, 1)
			} else if val == "state-2" {
				atomic.AddInt64(&queryNewStateCount, 1)
			}
		}()
	}

	// Simulate ContinueAsNew transition
	time.Sleep(5 * time.Millisecond)
	err := engine.ContinueAsNew("run-1", "run-2", "state-2")
	if err != nil {
		fmt.Printf("ContinueAsNew failed: %v\n", err)
	}

	wg.Wait()

	fmt.Printf("Total successful queries: %d\n", atomic.LoadInt64(&querySuccessCount))
	fmt.Printf("Queries returning old state: %d\n", atomic.LoadInt64(&queryStaleCount))
	fmt.Printf("Queries returning new state: %d\n", atomic.LoadInt64(&queryNewStateCount))

	if atomic.LoadInt64(&queryStaleCount)+atomic.LoadInt64(&queryNewStateCount) != atomic.LoadInt64(&querySuccessCount) {
		fmt.Println("Error: Inconsistent query results!")
	} else {
		fmt.Println("Success: All queries resolved correctly without returning stale state after ContinueAsNew committed.")
	}
}