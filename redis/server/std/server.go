package std

import (
	"context"
	"go-Redis/database"
	"go-Redis/redis/parser"
	"net"
	"strconv"
	"strings"
)

type Handler struct {
	dbSet *database.DBSet
}

func NewHandler() *Handler {
	return &Handler{
		dbSet: database.NewDBSet(),
	}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	ch := parser.ParseStream(conn)
	selectedDB := 0
	for payload := range ch {
		if payload.Err != nil {
			return
		}
		if len(payload.Data) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(payload.Data[0]))
		if cmd == "SELECT" {
			if len(payload.Data) != 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for SELECT\r\n"))
				continue
			}
			index, err := strconv.Atoi(string(payload.Data[1]))
			if err != nil {
				_, _ = conn.Write([]byte("-ERR invalid DB index\r\n"))
				continue
			}
			if h.dbSet.GetDB(index) == nil {
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			selectedDB = index
			_, _ = conn.Write([]byte("+OK\r\n"))
			continue
		}

		db := h.dbSet.GetDB(selectedDB)
		reply := db.Exec(payload.Data)
		_, _ = conn.Write(reply.ToBytes())

	}
}
func (h *Handler) Close() {

}
