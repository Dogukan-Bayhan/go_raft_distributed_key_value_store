package fsm

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