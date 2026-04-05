package database

import (
	"strings"
	"testing"
	"time"
)

func TestIncr(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("INCR"), []byte("count"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected incr create reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("INCR"), []byte("count"),
	}).ToBytes()); got != ":2\r\n" {
		t.Fatalf("unexpected incr second reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("GET"), []byte("count"),
	}).ToBytes()); got != "$1\r\n2\r\n" {
		t.Fatalf("unexpected incr get reply: %q", got)
	}
}
func TestDecr(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("DECR"), []byte("count"),
	}).ToBytes()); got != ":-1\r\n" {
		t.Fatalf("unexpected decr create reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("DECR"), []byte("count"),
	}).ToBytes()); got != ":-2\r\n" {
		t.Fatalf("unexpected decr second reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("GET"), []byte("count"),
	}).ToBytes()); got != "$2\r\n-2\r\n" {
		t.Fatalf("unexpected decr get reply: %q", got)
	}
}
func TestDBSize(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{[]byte("DBSIZE")}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected empty dbsize reply: %q", got)
	}

	db.Exec([][]byte{[]byte("SET"), []byte("a"), []byte("1")})
	db.Exec([][]byte{[]byte("SET"), []byte("b"), []byte("2")})

	if got := string(db.Exec([][]byte{[]byte("DBSIZE")}).ToBytes()); got != ":2\r\n" {
		t.Fatalf("unexpected dbsize reply: %q", got)
	}
}
func TestExpireConsistency(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{[]byte("SET"), []byte("temp"), []byte("value")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected set reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("EXPIRE"), []byte("temp"), []byte("1")}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected expire reply: %q", got)
	}

	time.Sleep(1100 * time.Millisecond)

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("temp")}).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("unexpected expired get reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("EXISTS"), []byte("temp")}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected expired exists reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("TYPE"), []byte("temp")}).ToBytes()); got != "+none\r\n" {
		t.Fatalf("unexpected expired type reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("DEL"), []byte("temp")}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected expired del reply: %q", got)
	}

	keysReply := string(db.Exec([][]byte{[]byte("KEYS"), []byte("*")}).ToBytes())
	if strings.Contains(keysReply, "temp") {
		t.Fatalf("expired key should not appear in KEYS reply: %q", keysReply)
	}
}

func TestSetClearsTTL(t *testing.T) {
	db := NewDB()

	db.Exec([][]byte{[]byte("SET"), []byte("k"), []byte("v1")})
	db.Exec([][]byte{[]byte("EXPIRE"), []byte("k"), []byte("1")})
	db.Exec([][]byte{[]byte("SET"), []byte("k"), []byte("v2")})

	time.Sleep(1100 * time.Millisecond)

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("k")}).ToBytes()); got != "$2\r\nv2\r\n" {
		t.Fatalf("set should clear ttl, got: %q", got)
	}
}

func TestHashCommands(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("HSET"), []byte("user"), []byte("name"), []byte("tom"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected hset first reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HSET"), []byte("user"), []byte("name"), []byte("jack"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected hset overwrite reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HGET"), []byte("user"), []byte("name"),
	}).ToBytes()); got != "$4\r\njack\r\n" {
		t.Fatalf("unexpected hget reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HGET"), []byte("user"), []byte("age"),
	}).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("unexpected missing field reply: %q", got)
	}
	if got := string(db.Exec([][]byte{
		[]byte("HEXISTS"), []byte("user"), []byte("name"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected hexists existing field reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HEXISTS"), []byte("user"), []byte("age"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected hexists missing field reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HEXISTS"), []byte("not_exist"), []byte("name"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected hexists missing key reply: %q", got)
	}
}

func TestHashWrongType(t *testing.T) {
	db := NewDB()

	db.Exec([][]byte{[]byte("SET"), []byte("k"), []byte("v")})

	got := string(db.Exec([][]byte{
		[]byte("HSET"), []byte("k"), []byte("field"), []byte("value"),
	}).ToBytes())

	if got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected wrong type reply: %q", got)
	}
	got = string(db.Exec([][]byte{
		[]byte("HEXISTS"), []byte("k"), []byte("field"),
	}).ToBytes())

	if got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected hexists wrong type reply: %q", got)
	}
}
func TestHDel(t *testing.T) {
	db := NewDB()

	db.Exec([][]byte{[]byte("HSET"), []byte("user"), []byte("name"), []byte("tom")})
	db.Exec([][]byte{[]byte("HSET"), []byte("user"), []byte("age"), []byte("18")})

	if got := string(db.Exec([][]byte{
		[]byte("HDEL"), []byte("user"), []byte("name"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected hdel single reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HEXISTS"), []byte("user"), []byte("name"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected hexists after hdel reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("HDEL"), []byte("user"), []byte("name"), []byte("age"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected hdel multi reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("TYPE"), []byte("user"),
	}).ToBytes()); got != "+none\r\n" {
		t.Fatalf("unexpected type after deleting all hash fields: %q", got)
	}
}
func TestLPush(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("LPUSH"), []byte("mylist"), []byte("a"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected lpush first reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("LPUSH"), []byte("mylist"), []byte("b"),
	}).ToBytes()); got != ":2\r\n" {
		t.Fatalf("unexpected lpush second reply: %q", got)
	}

	entity, ok := db.Data["mylist"]
	if !ok {
		t.Fatal("expected list key to exist")
	}

	if entity.Type != "list" {
		t.Fatalf("unexpected type: %s", entity.Type)
	}

	list, ok := entity.Data.([][]byte)
	if !ok {
		t.Fatal("expected list data type")
	}

	// LPUSH 先插 a，再左插 b，所以结果应该是 [b, a]。
	if len(list) != 2 || string(list[0]) != "b" || string(list[1]) != "a" {
		t.Fatalf("unexpected list contents: %#v", list)
	}
}
func TestDBExec(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{[]byte("PING")}).ToBytes()); got != "+PONG\r\n" {
		t.Fatalf("unexpected ping reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("SET"), []byte("name"), []byte("redis")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected set reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("name")}).ToBytes()); got != "$5\r\nredis\r\n" {
		t.Fatalf("unexpected get reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("not_exist")}).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("unexpected nil get reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("DEL"), []byte("name")}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected del reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("DEL"), []byte("not_exist")}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected del missing reply: %q", got)
	}
	if got := string(db.Exec([][]byte{[]byte("SET"), []byte("age"), []byte("18")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected second set reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("EXISTS"), []byte("name")}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected exists single reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("EXISTS"), []byte("age"), []byte("city")}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected exists multi reply: %q", got)
	}
	if got := string(db.Exec([][]byte{[]byte("SET"), []byte("city"), []byte("beijing")}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected third set reply: %q", got)
	}

	keysReply := string(db.Exec([][]byte{[]byte("KEYS"), []byte("*")}).ToBytes())
	if len(keysReply) == 0 || keysReply[0] != '*' {
		t.Fatalf("unexpected keys reply: %q", keysReply)
	}
	if !strings.Contains(keysReply, "age") || !strings.Contains(keysReply, "city") {
		t.Fatalf("keys reply missing expected keys: %q", keysReply)
	}

	if got := string(db.Exec([][]byte{[]byte("KEYS"), []byte("name*")}).ToBytes()); got != "-ERR only * pattern is supported\r\n" {
		t.Fatalf("unexpected keys pattern error reply: %q", got)
	}
	if got := string(db.Exec([][]byte{[]byte("TYPE"), []byte("age")}).ToBytes()); got != "+string\r\n" {
		t.Fatalf("unexpected type existing reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("TYPE"), []byte("not_exist")}).ToBytes()); got != "+none\r\n" {
		t.Fatalf("unexpected type missing reply: %q", got)
	}
	mgetReply := string(db.Exec([][]byte{[]byte("MGET"), []byte("age"), []byte("city"), []byte("not_exist")}).ToBytes())
	if len(mgetReply) == 0 || mgetReply[0] != '*' {
		t.Fatalf("unexpected mget reply: %q", mgetReply)
	}
	if !strings.Contains(mgetReply, "18") || !strings.Contains(mgetReply, "beijing") {
		t.Fatalf("mget reply missing expected values: %q", mgetReply)
	}
	if got := string(db.Exec([][]byte{
		[]byte("MSET"),
		[]byte("a"), []byte("1"),
		[]byte("b"), []byte("2"),
	}).ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected mset reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("a")}).ToBytes()); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected mset get a reply: %q", got)
	}

	if got := string(db.Exec([][]byte{[]byte("GET"), []byte("b")}).ToBytes()); got != "$1\r\n2\r\n" {
		t.Fatalf("unexpected mset get b reply: %q", got)
	}
	if got := string(db.Exec([][]byte{
		[]byte("MSET"),
		[]byte("x"), []byte("10"),
		[]byte("y"),
	}).ToBytes()); got != "-ERR wrong number of arguments for MSET\r\n" {
		t.Fatalf("unexpected mset arg error reply: %q", got)
	}

}
func TestRPush(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("RPUSH"), []byte("mylist"), []byte("a"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected rpush first reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("RPUSH"), []byte("mylist"), []byte("b"),
	}).ToBytes()); got != ":2\r\n" {
		t.Fatalf("unexpected rpush second reply: %q", got)
	}

	entity, ok := db.Data["mylist"]
	if !ok {
		t.Fatal("expected list key to exist")
	}

	if entity.Type != "list" {
		t.Fatalf("unexpected type: %s", entity.Type)
	}

	list, ok := entity.Data.([][]byte)
	if !ok {
		t.Fatal("expected list data type")
	}

	// RPUSH 先插 a，再右插 b，所以结果应该是 [a, b]。
	if len(list) != 2 || string(list[0]) != "a" || string(list[1]) != "b" {
		t.Fatalf("unexpected list contents: %#v", list)
	}
}
func TestZSetCommands(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("ZADD"), []byte("rank"), []byte("100"), []byte("tom"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected zadd first reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("ZADD"), []byte("rank"), []byte("90"), []byte("jack"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected zadd second reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("ZSCORE"), []byte("rank"), []byte("tom"),
	}).ToBytes()); got != "$3\r\n100\r\n" {
		t.Fatalf("unexpected zscore reply: %q", got)
	}

	reply := string(db.Exec([][]byte{
		[]byte("ZRANGE"), []byte("rank"), []byte("0"), []byte("-1"),
	}).ToBytes())

	if len(reply) == 0 || reply[0] != '*' {
		t.Fatalf("unexpected zrange reply: %q", reply)
	}

	// 按 score 从小到大，所以 jack(90) 在前，tom(100) 在后。
	if !strings.Contains(reply, "jack") || !strings.Contains(reply, "tom") {
		t.Fatalf("unexpected zrange contents: %q", reply)
	}
}
func TestLRange(t *testing.T) {
	db := NewDB()

	db.Exec([][]byte{[]byte("LPUSH"), []byte("mylist"), []byte("a")})
	db.Exec([][]byte{[]byte("LPUSH"), []byte("mylist"), []byte("b")})
	db.Exec([][]byte{[]byte("LPUSH"), []byte("mylist"), []byte("c")})

	reply := string(db.Exec([][]byte{
		[]byte("LRANGE"), []byte("mylist"), []byte("0"), []byte("1"),
	}).ToBytes())

	if len(reply) == 0 || reply[0] != '*' {
		t.Fatalf("unexpected lrange reply: %q", reply)
	}
	if !strings.Contains(reply, "c") || !strings.Contains(reply, "b") {
		t.Fatalf("unexpected lrange contents: %q", reply)
	}

	allReply := string(db.Exec([][]byte{
		[]byte("LRANGE"), []byte("mylist"), []byte("0"), []byte("-1"),
	}).ToBytes())

	if !strings.Contains(allReply, "a") || !strings.Contains(allReply, "b") || !strings.Contains(allReply, "c") {
		t.Fatalf("unexpected lrange all reply: %q", allReply)
	}
}
func TestSetCommands(t *testing.T) {
	db := NewDB()

	if got := string(db.Exec([][]byte{
		[]byte("SADD"), []byte("tags"), []byte("go"), []byte("redis"), []byte("go"),
	}).ToBytes()); got != ":2\r\n" {
		t.Fatalf("unexpected sadd reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("SISMEMBER"), []byte("tags"), []byte("go"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected sismember existing reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("SISMEMBER"), []byte("tags"), []byte("java"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected sismember missing reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("SREM"), []byte("tags"), []byte("go"),
	}).ToBytes()); got != ":1\r\n" {
		t.Fatalf("unexpected srem reply: %q", got)
	}

	if got := string(db.Exec([][]byte{
		[]byte("SISMEMBER"), []byte("tags"), []byte("go"),
	}).ToBytes()); got != ":0\r\n" {
		t.Fatalf("unexpected sismember after remove reply: %q", got)
	}
}
func TestWrongTypeCommands(t *testing.T) {
	db := NewDB()

	// 先准备 4 种不同类型的 key。
	db.Exec([][]byte{[]byte("SET"), []byte("str"), []byte("hello")})
	db.Exec([][]byte{[]byte("HSET"), []byte("hash"), []byte("name"), []byte("tom")})
	db.Exec([][]byte{[]byte("LPUSH"), []byte("list"), []byte("a")})
	db.Exec([][]byte{[]byte("ZADD"), []byte("zset"), []byte("100"), []byte("jack")})

	// string key 被 hash 命令操作，应该报错。
	if got := string(db.Exec([][]byte{
		[]byte("HGET"), []byte("str"), []byte("name"),
	}).ToBytes()); got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected hget on string reply: %q", got)
	}

	// hash key 被 string 命令操作，应该报错。
	if got := string(db.Exec([][]byte{
		[]byte("GET"), []byte("hash"),
	}).ToBytes()); got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected get on hash reply: %q", got)
	}

	// list key 被 zset 命令操作，应该报错。
	if got := string(db.Exec([][]byte{
		[]byte("ZADD"), []byte("list"), []byte("1"), []byte("x"),
	}).ToBytes()); got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected zadd on list reply: %q", got)
	}

	// zset key 被 list 命令操作，应该报错。
	if got := string(db.Exec([][]byte{
		[]byte("LPUSH"), []byte("zset"), []byte("x"),
	}).ToBytes()); got != "-ERR wrong type\r\n" {
		t.Fatalf("unexpected lpush on zset reply: %q", got)
	}
}
