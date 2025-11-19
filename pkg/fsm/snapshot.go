package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
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

	dataBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(dataBuf).Encode(kv); err != nil {
		return err
	}

	dataBytes := dataBuf.Bytes()
	dataSize() := uint32(len(dataBytes))
	dataChecksum := crc32.ChecksumIEEE(dataBytes)


	fpath := fmt.Sprintf("%s/snapshot_%d.snap", path, meta.Index)
	file, err := os.OpenFile(fpath, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	 if _, err := file.Write(metaBytes); err != nil {
        return err
    }
    if err := writeUint32(file, metaSize); err != nil {
        return err
    }
    if err := writeUint32(file, metaChecksum); err != nil {
        return err
    }

    // --- DATA ---
    if _, err := file.Write(dataBytes); err != nil {
        return err
    }
    if err := writeUint32(file, dataSize); err != nil {
        return err
    }
    if err := writeUint32(file, dataChecksum); err != nil {
        return err
    }

    return nil
}