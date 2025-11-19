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
