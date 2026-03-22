package main

import (
	"go-Redis/interface/tcp"
	std "go-Redis/redis/server/std"
)

func main() {
	addr := "127.0.0.1:6311"
	handler := std.NewHandler()
	err := tcp.ListenAndServe(addr, handler)
	if err != nil {
		panic(err)
	}

}
