package fsm


func (f *FSM) Apply(msg ApplyMsg) interface{} {
    cmd, err := DecodeCommand(msg.Data)
    if err != nil { panic(err) }

    switch cmd.Op {
    case OpPUT:
        f.applyPut(cmd)
    case OpDEL:
        f.applyDelete(cmd)
    case OpTTLPut:
        f.applyTTLPut(cmd)
    case OpCAS:
        ok := f.applyCAS(cmd)
		return ok
    case OpGCExpire:
        f.applyExpire(cmd)
    case OpBATCH:
        f.applyBatch(cmd.Ops)
    }

    // update metrics
    f.updateMetrics(cmd)
	return nil
}

// --- METRICS ---

func (f *FSM) updateMetrics(cmd Command) {
    // Şimdilik boş
}

// --- APPLY OPERATIONS ---

func (f *FSM) applyPut(cmd Command) {
    // TODO: storage.Put
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

