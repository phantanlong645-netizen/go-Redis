package std

import (
	"bufio"
	"context"
	"io"
	"net"
)

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		_, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		_, _ = conn.Write([]byte("+PONG\r\n"))
	}
}
func (h *Handler) Close() {

}
