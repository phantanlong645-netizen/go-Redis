package tcp

import (
	"context"
	"net"
)

func ListenAndServe(addr string, handler handler) error {

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx := context.Background()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handler.Handle(ctx, conn)
	}
}
