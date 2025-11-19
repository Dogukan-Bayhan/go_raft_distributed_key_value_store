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
	metaChecksum := crc32.ChecksumIEEE(metaBytes)

	dataBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(dataBuf).Encode(kv); err != nil {
		return err
	}

	dataBytes := dataBuf.Bytes()
	dataSize := uint32(len(dataBytes))
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

func (f *FSM) RestoreSnapshot(path string) (*SnapshotMeta, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    buf, err := io.ReadAll(file)
    if err != nil {
        return nil, err
    }

    r := bytes.NewReader(buf)

    dataChecksum, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

	dataSize, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

    end := len(buf) - 8
    dataStart := end - int(dataSize)
    dataBytes := buf[dataStart:end]

    if crc32.ChecksumIEEE(dataBytes) != dataChecksum {
        return nil, fmt.Errorf("snapshot corrupted: data checksum mismatch")
    }

    var kv map[string][]byte
    if err := gob.NewDecoder(bytes.NewReader(dataBytes)).Decode(&kv); err != nil {
        return nil, err
    }

    metaChecksum, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

	metaSize, err := readUint32FromEnd(r)
    if err != nil {
        return nil, err
    }

    metaEnd := dataStart - 8
    metaStart := metaEnd - int(metaSize)
    metaBytes := buf[metaStart:metaEnd]

    if crc32.ChecksumIEEE(metaBytes) != metaChecksum {
        return nil, fmt.Errorf("snapshot corrupted: meta checksum mismatch")
    }

    var meta SnapshotMeta
    if err := gob.NewDecoder(bytes.NewReader(metaBytes)).Decode(&meta); err != nil {
        return nil, err
    }

    if err := f.Storage.Restore(kv); err != nil {
        return nil, err
    }

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
