package faults

import (
	"context"
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------
// InMemoryDataStore — a concrete, usable DataStore.
//
// This is real bookkeeping (per-node logs and per-node/table/key rows in
// memory), not a stub — you can inject/revert against it and read the
// results back. What it is NOT is an adapter to a real engine: it holds
// its own copy of "what the log/rows look like", so injecting against it
// proves the fault logic is correct but doesn't touch an actual Postgres
// table or a real Raft node's log. Wiring this to real storage means
// writing a second DataStore implementation (e.g. one backed by a
// Postgres connection for ReadRow/WriteRow, and by dbfailsim's own
// Raft-lite log once it exists for ReadLogEntries/WriteLogEntry) with the
// exact same interface — DataFault code above doesn't change either way.
type InMemoryDataStore struct {
	mu   sync.Mutex
	logs map[string][]LogEntry          // nodeID -> log
	rows map[string]map[string][]byte   // nodeID -> "table:key" -> value
}

func NewInMemoryDataStore() *InMemoryDataStore {
	return &InMemoryDataStore{
		logs: make(map[string][]LogEntry),
		rows: make(map[string]map[string][]byte),
	}
}

func rowKey(table, key string) string { return table + ":" + key }

func (s *InMemoryDataStore) ReadLogEntries(ctx context.Context, nodeID string, fromIndex uint64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []LogEntry
	for _, e := range s.logs[nodeID] {
		if e.Index >= fromIndex {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *InMemoryDataStore) WriteLogEntry(ctx context.Context, nodeID string, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs[nodeID] = append(s.logs[nodeID], entry)
	return nil
}

func (s *InMemoryDataStore) ReadRow(ctx context.Context, nodeID, table, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.rows[nodeID]
	if !ok {
		return nil, fmt.Errorf("no rows for node %s", nodeID)
	}
	v, ok := node[rowKey(table, key)]
	if !ok {
		return nil, fmt.Errorf("no row %s/%s on node %s", table, key, nodeID)
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *InMemoryDataStore) WriteRow(ctx context.Context, nodeID, table, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[nodeID] == nil {
		s.rows[nodeID] = make(map[string][]byte)
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	s.rows[nodeID][rowKey(table, key)] = cp
	return nil
}

// ---------------------------------------------------------------------
// Log divergence — overwrites a target node's log tail with entries
// carrying a different Term than the source's, simulating an
// un-reconciled split after a network partition: two nodes both accepted
// writes for the same index range under different terms, and haven't
// run the real consensus repair (log truncation + re-replication) yet.
// ---------------------------------------------------------------------

type LogDivergenceFault struct {
	SourceNode string // the node whose log we branch from
	TargetNode string // the node that gets the diverged tail
	FromIndex  uint64
	FakeTerm   uint64 // term to stamp on the diverged entries

	appliedFrom uint64
	appliedTo   uint64
}

func (f *LogDivergenceFault) Name() string { return "log_divergence" }

func (f *LogDivergenceFault) Inject(ctx context.Context, store DataStore, nodeID string) error {
	entries, err := store.ReadLogEntries(ctx, f.SourceNode, f.FromIndex)
	if err != nil {
		return fmt.Errorf("log_divergence: reading source log: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("log_divergence: no entries on %s from index %d", f.SourceNode, f.FromIndex)
	}

	f.appliedFrom = entries[0].Index
	f.appliedTo = entries[len(entries)-1].Index

	for _, e := range entries {
		diverged := LogEntry{
			Index: e.Index,
			Term:  f.FakeTerm, // different term, same index -> a real conflict
			Data:  append([]byte(nil), e.Data...),
		}
		if err := store.WriteLogEntry(ctx, f.TargetNode, diverged); err != nil {
			return fmt.Errorf("log_divergence: writing diverged entry at index %d: %w", e.Index, err)
		}
	}
	return nil
}

// Revert is a documented no-op: log divergence is meant to be resolved
// by the system under test's own conflict-resolution path (log
// truncation + catch-up), not undone by the fault. Calling Revert tells
// you whether that repair actually happened — compare TargetNode's log
// at appliedFrom..appliedTo against SourceNode's afterward.
func (f *LogDivergenceFault) Revert(ctx context.Context, store DataStore, nodeID string) error {
	return nil
}

// ---------------------------------------------------------------------
// Silent row corruption — flips bytes in a stored row on one replica
// without going through any error path, simulating bit rot or a storage
// bug that produces wrong data with no accompanying failure signal.
// ---------------------------------------------------------------------

type RowCorruptionFault struct {
	Table       string
	Key         string
	BytesToFlip int

	previousValue []byte
	hadPrevious   bool
}

func (f *RowCorruptionFault) Name() string { return "row_corruption" }

func (f *RowCorruptionFault) Inject(ctx context.Context, store DataStore, nodeID string) error {
	live, err := store.ReadRow(ctx, nodeID, f.Table, f.Key)
	if err != nil {
		return fmt.Errorf("row_corruption: reading row: %w", err)
	}
	f.previousValue = append([]byte(nil), live...)
	f.hadPrevious = true

	corrupted := flipBytes(live, f.BytesToFlip)
	if err := store.WriteRow(ctx, nodeID, f.Table, f.Key, corrupted); err != nil {
		return fmt.Errorf("row_corruption: writing corrupted row: %w", err)
	}
	return nil
}

func (f *RowCorruptionFault) Revert(ctx context.Context, store DataStore, nodeID string) error {
	if !f.hadPrevious {
		return nil
	}
	return store.WriteRow(ctx, nodeID, f.Table, f.Key, f.previousValue)
}