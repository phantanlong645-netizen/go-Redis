package std

import (
	"context"
	"go-Redis/redis/parser"
	"go-Redis/redis/protocol"
	"net"
	"strings"
)

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	ch := parser.ParseStream(conn)
	for paload := range ch {
		if paload.Err != nil {
			return
		}
		if len(paload.Data) == 0 {
			continue
		}
		cmd := strings.ToUpper(string(paload.Data[0]))
		if cmd == "PING" {
			_, _ = conn.Write(protocol.NewStatusReply("PONG").ToBytes())
			continue
		}
		_, _ = conn.Write(protocol.NewErrReply("ERR unknown command").ToBytes())
	}

}
func (h *Handler) Close() {

}
