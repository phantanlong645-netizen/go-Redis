package protocol

type Reply interface {
	ToBytes() []byte
}
type StatusReply struct {
	Status string
}
type ErrReply struct {
	Err string
}

func NewErrReply(err string) *ErrReply {
	return &ErrReply{
		Err: err,
	}
}
func (errReply *ErrReply) ToBytes() []byte {
	return []byte("-" + errReply.Err + "\r\n")
}

func NewStatusReply(reply string) *StatusReply {
	return &StatusReply{
		Status: reply,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + "\r\n")
}
