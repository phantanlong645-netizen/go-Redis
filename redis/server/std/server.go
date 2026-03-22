package std

import (
	"context"
	"go-Redis/database"
	"go-Redis/redis/parser"
	"net"
)

type Handler struct {
	db *database.DB
}

func NewHandler() *Handler {
	return &Handler{
		db: database.NewDB(),
	}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			return
		}
		if len(payload.Data) == 0 {
			continue
		}
		reply := h.db.Exec(payload.Data)

		_, _ = conn.Write(reply.ToBytes())
	}
}
func (h *Handler) Close() {

}
