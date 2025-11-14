package fsm

import "time"

func (f *FSM) Apply(msg ApplyMsg) interface{} {
	if msg.Index <= f.LastApplied {
		return nil
	}

	start := time.Now()

	cmd, err := DecodeCommand(msg.Data)
	if err != nil {
		panic(err)
	}

    var result interface{}
	switch cmd.Op {
	case OpPUT:
		f.applyPut(cmd)
	case OpDEL:
		f.applyDelete(cmd)
	case OpTTLPut:
		f.applyTTLPut(cmd)
	case OpCAS:
		result = f.applyCAS(cmd)
	case OpGCExpire:
		f.applyExpire(cmd)
	case OpBATCH:
		f.applyBatch(cmd.Ops)
	}

	f.LastApplied = msg.Index

	// update metrics
	f.updateMetrics(cmd, time.Since(start))
	return result
}

// --- METRICS ---

func (f *FSM) updateMetrics(cmd Command, latency time.Duration) {
	lat := uint64(latency.Nanoseconds())

    f.Metrics.ApplyCount++
    count := f.Metrics.ApplyCount

    f.Metrics.ApplyAvgLatencyNs =
    ((f.Metrics.ApplyAvgLatencyNs * (count - 1)) + lat) / count
}

// --- APPLY OPERATIONS ---

func (f *FSM) applyPut(cmd Command) {
    
}

func (f *FSM) applyDelete(cmd Command) {
	// TODO: storage.Delete
}

func (f *FSM) applyTTLPut(cmd Command) {
	// TODO: TTL hesapla + storage.Put
}

func (f *FSM) applyExpire(cmd Command) {
	// TODO: TTL expiration için delete
}

func (f *FSM) applyBatch(cmds []Command) {
	// TODO: Apply every command
	for _, c := range cmds {
		switch c.Op {
		case OpPUT:
			f.applyPut(c)
		case OpDEL:
			f.applyDelete(c)
		case OpTTLPut:
			f.applyTTLPut(c)
		case OpCAS:
			_ = f.applyCAS(c)
		case OpGCExpire:
			f.applyExpire(c)
		}
	}
}

func (f *FSM) applyCAS(cmd Command) bool {
	// TODO: compare-and-set
	return false
}
