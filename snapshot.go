package main

import "github.com/hashicorp/raft"

type snapshot struct {
	//cm *cacheManager``
}

// Persist saves the FSM snapshot out to the given sink.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	sink.Close()
	return nil
}

func (f *snapshot) Release() {}
