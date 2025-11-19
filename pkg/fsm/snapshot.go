package fsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// CreateSnapshot serializes the FSM's deterministic state into a durable,
// verifiable snapshot file. Snapshots allow the system to discard old WAL
// segments and perform fast crash recovery.
//
// The snapshot file has the following footer-oriented binary layout:
//
//   [ META_BYTES ][ META_SIZE ][ META_CHECKSUM ]
//   [ DATA_BYTES ][ DATA_SIZE ][ DATA_CHECKSUM ]
//
// Footers (SIZE + CHECKSUM) are written immediately *after* each block so
// RestoreSnapshot can seek from the end of the file and locate blocks in
// O(1) time without scanning. This is a standard design used in LSM/raft
// storage engines.
//
// META block:
//   - Contains SnapshotMeta (last applied index, term, timestamp, etc.)
// DATA block:
//   - Full serialized KV-state returned by Storage.Snapshot()
//
// Both blocks are gob-encoded and protected using CRC32-IEEE checksums.
// If either checksum does not match during restoration, the snapshot is
// considered corrupted and rejected.
func (f *FSM) CreateSnapshot(path string, meta SnapshotMeta) error {

    // Capture a consistent KV view from the storage backend.
    kv, err := f.Storage.Snaphsot()
    if err != nil {
        return err
    }

    // Timestamp the snapshot. Raft will fill Index and Term.
    meta.Created = time.Now()

    // --- Encode META block ---
    metaBuf := new(bytes.Buffer)
    if err := gob.NewEncoder(metaBuf).Encode(meta); err != nil {
        return err
    }
    metaBytes := metaBuf.Bytes()
    metaSize := uint32(len(metaBytes))
    metaChecksum := crc32.ChecksumIEEE(metaBytes)

    // --- Encode DATA block ---
    dataBuf := new(bytes.Buffer)
    if err := gob.NewEncoder(dataBuf).Encode(kv); err != nil {
        return err
    }
    dataBytes := dataBuf.Bytes()
    dataSize := uint32(len(dataBytes))
    dataChecksum := crc32.ChecksumIEEE(dataBytes)

    // Snapshot file name includes the last applied index to maintain ordering.
    fpath := fmt.Sprintf("%s/snapshot_%d.snap", path, meta.Index)

    file, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    // --- Write META block ---
    // Layout: [metaBytes][metaSize][metaChecksum]
    if _, err := file.Write(metaBytes); err != nil {
        return err
    }
    if err := writeUint32(file, metaSize); err != nil {
        return err
    }
    if err := writeUint32(file, metaChecksum); err != nil {
        return err
    }

    // --- Write DATA block ---
    // Layout: [dataBytes][dataSize][dataChecksum]
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

// RestoreSnapshot loads a snapshot from disk, verifies its integrity,
// decodes META and DATA blocks, and installs the KV-state into the FSM.
// The layout is parsed backwards to achieve constant-time block detection:
//
//   from end → DATA_CHECKSUM → DATA_SIZE → DATA_BYTES
//             META_CHECKSUM → META_SIZE → META_BYTES
//
// This avoids scanning the file and is the standard approach used by
// RocksDB, Badger, and Hashicorp Raft.
func (f *FSM) RestoreSnapshot(path string) (*SnapshotMeta, error) {

    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    // Read entire snapshot into memory (snapshot files are typically small).
    buf, err := io.ReadAll(file)
    if err != nil {
        return nil, err
    }
    r := bytes.NewReader(buf)

    // --- Read DATA footer ---
    dataChecksum, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }
    dataSize, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

    // Compute byte ranges for DATA block.
    dataEnd := len(buf) - 8
    dataStart := dataEnd - int(dataSize)
    dataBytes := buf[dataStart:dataEnd]

    // Validate DATA checksum.
    if crc32.ChecksumIEEE(dataBytes) != dataChecksum {
        return nil, fmt.Errorf("snapshot corrupted: data checksum mismatch")
    }

    // Decode KV-state.
    var kv map[string][]byte
    if err := gob.NewDecoder(bytes.NewReader(dataBytes)).Decode(&kv); err != nil {
        return nil, err
    }

    // --- Read META footer ---
    metaChecksum, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }
    metaSize, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

    // Compute byte ranges for META block.
    metaEnd := dataStart - 8
    metaStart := metaEnd - int(metaSize)
    metaBytes := buf[metaStart:metaEnd]

    // Validate META checksum.
    if crc32.ChecksumIEEE(metaBytes) != metaChecksum {
        return nil, fmt.Errorf("snapshot corrupted: meta checksum mismatch")
    }

    // Decode snapshot metadata.
    var meta SnapshotMeta
    if err := gob.NewDecoder(bytes.NewReader(metaBytes)).Decode(&meta); err != nil {
        return nil, err
    }

    // Reinstall KV-state into the storage backend.
    if err := f.Storage.Restore(kv); err != nil {
        return nil, err
    }

    // Update FSM apply pointer.
    f.LastApplied = meta.Index

    return &meta, nil
}


func writeUint32(w io.Writer, v uint32) error {
    var buf [4]byte
    buf[0] = byte(v >> 24)
    buf[1] = byte(v >> 16)
    buf[2] = byte(v >> 8)
    buf[3] = byte(v)
    _, err := w.Write(buf[:])
    return err
}

func readUint32FromEnd(r *bytes.Reader) (uint32, error) {
    // sondan 4 byte geri git
    offset, _ := r.Seek(-4, io.SeekEnd)
    var buf [4]byte
    if _, err := r.Read(buf[:]); err != nil {
        return 0, err
    }
    // tekrar 4 byte geri (sonrakine hazırlık)
    r.Seek(offset, io.SeekStart)

    v := uint32(buf[0])<<24 |
         uint32(buf[1])<<16 |
         uint32(buf[2])<<8  |
         uint32(buf[3])
    return v, nil
}
