package protocol

type Reply interface {
	ToBytes() []byte
}
type StatusReply struct {
	Status string
}

func NewStatusReply(reply string) *StatusReply {
	return &StatusReply{
		Status: reply,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + "\r\n")
}
