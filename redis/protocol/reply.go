package protocol

import "strconv"

type Reply interface {
	ToBytes() []byte
}
type StatusReply struct {
	Status string
}
type ErrReply struct {
	Err string
}
type BulkReply struct {
	Arg []byte
}
type NoReply struct {
}

type NullBulkReply struct {
}
type IntReply struct {
	Code int64
}
type MultiBulkReply struct {
	Args [][]byte
}

func (m *MultiBulkReply) ToBytes() []byte {
	res := []byte("*" + strconv.Itoa(len(m.Args)) + "\r\n")
	for _, arg := range m.Args {
		res = append(res, []byte("$"+strconv.Itoa(len(arg))+"\r\n")...)
		res = append(res, arg...)
		res = append(res, []byte("\r\n")...)
	}
	return res
}
func NewMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
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

func NewBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}
func NewNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}
func NewIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + "\r\n")
}
func (Reply *BulkReply) ToBytes() []byte {
	return []byte("$" + strconv.Itoa(len(Reply.Arg)) + "\r\n" + string(Reply.Arg) + "\r\n")
}
func (Reply *NullBulkReply) ToBytes() []byte {
	return []byte("$-1\r\n")
}

func (Reply *NoReply) ToBytes() []byte {
	return nil
}
