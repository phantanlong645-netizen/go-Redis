package tcp

import (
	"context"
	"net"
)

type handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close()
}
