package fsm

import (
	"bytes"
	"encoding/gob"
)

type OpCode uint8

const (
	OpPUT OpCode = iota
	OpDEL
	OpCAS
	OpBATCH
	OpTTLPut
	OpGCExpire
)

type Command struct {
	Version uint16
	Op      OpCode
	Key     []byte
	Value   []byte
	CASOld  []byte
	TTLms   uint32
	Ops     []Command
}

func EncodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeCommand(b []byte) (Command, error) {
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}