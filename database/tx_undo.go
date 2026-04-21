package database

import (
	list2 "go-Redis/datastruct/list"
	set2 "go-Redis/datastruct/set"
	zset2 "go-Redis/datastruct/zset"
	"sort"
	"strconv"
	"time"
)

func undoSet(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoDel(db *DB, cmdLine [][]byte) [][][]byte {
	res := make([][][]byte, 0)
	for _, arg := range cmdLine {
		key := string(arg)
		res = append(res, undoForkey(db, key)...)
	}
	return res
}

func undoMSet(db *DB, cmdLine [][]byte) [][][]byte {
	res := make([][][]byte, 0)
	for i := 0; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		res = append(res, undoForkey(db, key)...)
	}
	return res
}

func undoExpire(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoPersist(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoIncr(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoDecr(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoPExpireAt(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}
func undoHSet(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoHDel(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoLPush(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoLPop(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoRPush(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoSAdd(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoSRem(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoZAdd(db *DB, cmdLine [][]byte) [][][]byte {
	key := string(cmdLine[0])
	return undoForkey(db, key)
}

func undoForkey(db *DB, key string) [][][]byte {
	entity, expireAt, ok := getUndoSnapshot(db, key)
	if !ok {
		return [][][]byte{
			{
				[]byte("DEL"),
				[]byte(key),
			},
		}
	}
	res := make([][][]byte, 0)
	switch entity.Type {
	case "string":
		value, _ := entity.Data.([]byte)
		res = append(res, [][]byte{
			[]byte("SET"),
			[]byte(key),
			append([]byte(nil), value...),
		})
	case "list":
		list, _ := entity.Data.(list2.List)
		cmd := make([][]byte, 0, list.Len()+2)
		cmd = append(cmd, []byte("DEL"), []byte(key))
		res = append(res, cmd)

		if list.Len() > 0 {
			pushCmd := make([][]byte, 0, list.Len()+1)
			pushCmd = append(pushCmd, []byte("RPUSH"), []byte(key))
			values := list.Range(0, list.Len())
			for _, v := range values {
				pushCmd = append(pushCmd, append([]byte(nil), v.([]byte)...))
			}
			res = append(res, pushCmd)
		}
	case "set":
		set, _ := entity.Data.(*set2.Set)
		res = append(res, [][]byte{
			[]byte("DEL"),
			[]byte(key),
		})
		if set != nil && set.Len() > 0 {
			members := set.Members()
			sort.Strings(members)
			cmd := make([][]byte, 0, len(members)+2)
			cmd = append(cmd, []byte("SADD"), []byte(key))
			for _, member := range members {
				cmd = append(cmd, []byte(member))
			}
			res = append(res, cmd)

		}
	case "hash":
		hash, _ := entity.Data.(map[string][]byte)
		fields := make([]string, 0, len(hash))
		for k := range hash {
			fields = append(fields, k)
		}
		sort.Strings(fields)
		res = append(res, [][]byte{
			[]byte("DEL"),
			[]byte(key),
		})
		for _, field := range fields {
			res = append(res, [][]byte{
				[]byte("HSET"),
				[]byte(key),
				[]byte(field),
				append([]byte(nil), hash[field]...),
			})
		}
	case "zset":
		zset, _ := entity.Data.(*zset2.SortedSet)
		res = append(res, [][]byte{
			[]byte("DEL"),
			[]byte(key),
		})
		if zset != nil && zset.Len() > 0 {
			for rank := int64(1); rank <= zset.Len(); rank++ {
				element, ok := zset.GetByRank(rank)
				if !ok {
					continue
				}
				res = append(res, [][]byte{
					[]byte("ZADD"),
					[]byte(key),
					[]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)),
					[]byte(element.Member),
				})
			}
		}
	}
	if !expireAt.IsZero() {
		res = append(res, [][]byte{
			[]byte("PEXPIREAT"),
			[]byte(key),
			[]byte(strconv.FormatInt(expireAt.UnixMilli(), 10)),
		})
	}

	return res

}
func getUndoSnapshot(db *DB, key string) (*DataEntity, time.Time, bool) {
	expireAt, hasTTL := db.TTL[key]
	if hasTTL && time.Now().After(expireAt) {
		return nil, time.Time{}, false
	}
	entity, ok := db.Data[key]
	if !ok {
		return nil, time.Time{}, false
	}
	if !hasTTL {
		return entity, time.Time{}, true
	}
	return entity, expireAt, true
}
