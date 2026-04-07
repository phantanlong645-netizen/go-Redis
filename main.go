package main

import (
	"go-Redis/interface/tcp"
	std "go-Redis/redis/server/std"
	"os"
)

func main() {
	addr := "127.0.0.1:6311"
	filename := "appendonly.aof"

	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		filename = os.Args[2]
	}

	handler := std.NewHandlerWithAOF(filename)
	err := tcp.ListenAndServe(addr, handler)
	if err != nil {
		panic(err)
	}
}
