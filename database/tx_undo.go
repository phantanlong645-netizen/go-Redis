package database

import (
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
	return nil
}

func undoPersist(db *DB, cmdLine [][]byte) [][][]byte {
	return nil
}

func undoIncr(db *DB, cmdLine [][]byte) [][][]byte {
	return nil
}

func undoDecr(db *DB, cmdLine [][]byte) [][][]byte {
	return nil
}

func undoPExpireAt(db *DB, cmdLine [][]byte) [][][]byte {
	return nil
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
		list, _ := entity.Data.([][]byte)
		cmd := make([][]byte, 0, len(list)+2)
		cmd = append(cmd, []byte("DEL"), []byte(key))
		res = append(res, cmd)

		if len(list) > 0 {
			pushCmd := make([][]byte, 0, len(list)+1)
			pushCmd = append(pushCmd, []byte("RPUSH"), []byte(key))
			for _, v := range list {
				pushCmd = append(pushCmd, append([]byte(nil), v...))
			}
			res = append(res, pushCmd)
		}
	case "set":
		set, _ := entity.Data.(map[string]struct{})
		members := make([]string, 0, len(set))
		for k := range set {
			members = append(members, k)
		}
		sort.Strings(members)
		res = append(res, [][]byte{
			[]byte("DEL"),
			[]byte(key),
		})
		if len(members) > 0 {
			cmd := make([][]byte, 0, len(members)+2)
			cmd = append(cmd, []byte("SADD"), []byte(key))
			for _, v := range members {
				cmd = append(cmd, []byte(v))
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
		zset, _ := entity.Data.(map[string]float64)
		members := make([]string, 0, len(zset))
		for member := range zset {
			members = append(members, member)
		}
		sort.Strings(members)
		res = append(res, [][]byte{
			[]byte("DEL"),
			[]byte(key),
		})
		for _, member := range members {
			res = append(res, [][]byte{
				[]byte("ZADD"),
				[]byte(key),
				[]byte(strconv.FormatFloat(zset[member], 'f', -1, 64)),
				[]byte(member),
			})
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
