package std

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestPubSubWalkthrough(t *testing.T) {
	dir := t.TempDir()
	handler := NewHandlerWithAOF(filepath.Join(dir, "appendonly.aof"))
	defer handler.Close()

	// 这一对 Pipe 模拟“订阅者客户端 <-> 服务端”之间的一条 TCP 连接。
	// subClientConn 站在测试代码这一侧，负责发命令、读回包；
	// subServerConn 交给 handler.Handle，模拟真正的服务端在处理这个客户端连接。
	subServerConn, subClientConn := net.Pipe()
	defer subClientConn.Close()
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		handler.Handle(context.Background(), subServerConn)
	}()
	subReader := bufio.NewReader(subClientConn)

	// 这一对 Pipe 模拟“发布者客户端 <-> 服务端”的另一条独立连接。
	// 之所以要单独再建一条连接，是因为 Pub/Sub 的典型场景就是：
	// 一个客户端负责订阅，另一个客户端负责发布。
	pubServerConn, pubClientConn := net.Pipe()
	defer pubClientConn.Close()
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		handler.Handle(context.Background(), pubServerConn)
	}()
	pubReader := bufio.NewReader(pubClientConn)

	// 给订阅者连接设置超时，避免测试因为读写阻塞而无限挂住。
	if err := subClientConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set subscriber deadline failed: %v", err)
	}

	// 订阅者发送 SUBSCRIBE news。
	// 这条命令会走：
	// server.Handle -> Exec -> pubsub.Subscribe -> subscribe0
	// 然后服务端会立即回一条订阅确认消息，而不是普通的 +OK。
	if _, err := subClientConn.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatalf("write subscribe failed: %v", err)
	}

	// 读取订阅确认消息。
	// 这条 RESP 数组的三个元素分别表示：
	// 1. "subscribe"：说明这是订阅确认
	// 2. "news"：说明订阅的是哪个频道
	// 3. 1：说明这个客户端当前总共订阅了 1 个频道
	subscribeReply := readRESPReply(t, subReader)
	wantSubscribe := "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
	if subscribeReply != wantSubscribe {
		t.Fatalf("unexpected subscribe reply: got %q want %q", subscribeReply, wantSubscribe)
	}

	// 给发布者连接也设置超时，避免测试在发布链路上卡死。
	if err := pubClientConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set publisher deadline failed: %v", err)
	}

	// 发布者发送 PUBLISH news hello。
	// 这条命令会走：
	// server.Handle -> Exec -> pubsub.Publish
	// 服务端会先找到订阅了 news 的所有连接，然后把 message 推给它们。
	if _, err := pubClientConn.Write([]byte("PUBLISH news hello\r\n")); err != nil {
		t.Fatalf("write publish failed: %v", err)
	}

	// 订阅者不是收到一个普通命令回复，而是收到一条“推送消息”：
	// ["message", "news", "hello"]
	// 这正是 Pub/Sub 和普通 GET/SET 最大的不同：消息是服务端主动推过来的。
	messageReply := readRESPReply(t, subReader)
	wantMessage := "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
	if messageReply != wantMessage {
		t.Fatalf("unexpected pushed message: got %q want %q", messageReply, wantMessage)
	}

	// 发布者自己还会收到 PUBLISH 命令的返回值。
	// 这里的 :1 表示有 1 个订阅者收到了这条消息。
	// 注意这和订阅者收到的推送消息不是同一条数据，而是发布命令本身的执行结果。
	publishReply := readRESPReply(t, pubReader)
	wantPublish := ":1\r\n"
	if publishReply != wantPublish {
		t.Fatalf("unexpected publish reply: got %q want %q", publishReply, wantPublish)
	}

	// 关闭两个客户端连接，等待服务端 goroutine 正常退出，避免测试泄漏 goroutine。
	_ = subClientConn.Close()
	_ = pubClientConn.Close()
	<-subDone
	<-pubDone
}
