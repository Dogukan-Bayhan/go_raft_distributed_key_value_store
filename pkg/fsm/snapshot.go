package fsm

import (
	"bytes"
	"encoding/gob"
	"hash/crc32"
	"time"
)

func (f *FSM) CreateSnapshot(path string, meta SnapshotMeta) error {

	kv, err := f.Storage.Snaphsot()
	if err != nil {
		return err
	}

	meta.Created = time.Now()

	metaBuf := new(bytes.Buffer)

	if err := gob.NewEncoder(metaBuf).Encode(meta); err != nil {
		return err
	}

	metaBytes :=  metaBuf.Bytes()
	metaSize := uint32(len(metaBytes))
	metaChechksum := crc32.ChecksumIEEE(metaBytes)
}