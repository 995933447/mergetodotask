package mergetodotask

import (
	"errors"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

var redisPool *redis.Pool

// ExecRedisCmd 可以自定义redis执行函数覆盖默认的执行函数
var ExecRedisCmd = func(ttl int64, cmd, key string, args ...interface{}) (interface{}, error) {
	return execRedisCmd(ttl, cmd, key, args...)
}

func execRedisCmd(ttl int64, cmd, key string, args ...interface{}) (interface{}, error) {
	conn := redisPool.Get()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	defer conn.Close()

	reply, err := conn.Do(cmd, append([]interface{}{key}, args...)...)
	if err == nil {
		if ttl > 0 {
			_, _ = conn.Do("EXPIRE", key, ttl)
		}
	}

	return reply, err
}

func execRedisSMembers(key string, args ...interface{}) ([]int64, error) {
	list, err := redis.Int64s(ExecRedisCmd(0, "SMEMBERS", key, args...))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, nil
		}
		return nil, err
	}
	return list, nil
}

func execZRemRangeByScore(key string, start interface{}, end interface{}) error {
	_, err := ExecRedisCmd(0, "ZREMRANGEBYSCORE", key, start, end)
	if err != nil {
		return err
	}
	return nil
}

func execRedisZIncrBy(key string, inc int64, data interface{}, ttl int64) error {
	_, err := ExecRedisCmd(ttl, "ZINCRBY", key, inc, data)
	if err != nil {
		return err
	}

	return nil
}

func execRedisSAdd(key string, data interface{}, ttl int64) error {
	_, err := ExecRedisCmd(ttl, "SADD", key, data)
	if err != nil {
		return err
	}

	return nil
}

func execZScanWithScore(key string, cursor, count int64) (int64, map[string]int64, error) {
	mapKeyToScore := map[string]int64{}

	res, err := redis.Values(ExecRedisCmd(0, "ZSCAN", key, cursor, "COUNT", count))
	if err != nil {
		return 0, nil, err
	}

	cursor, err = redis.Int64(res[0], nil)
	if err != nil {
		return 0, nil, err
	}

	keyWithScores, err := redis.Strings(res[1], nil)
	if err != nil {
		return 0, nil, err
	}

	var (
		k     string
		score int64
	)
	size := len(keyWithScores)
	for i := 1; i <= size; i++ {
		keyOrScore := keyWithScores[i-1]
		if i%2 == 0 {
			score, err = strconv.ParseInt(keyOrScore, 10, 64)
			if err != nil {
				return 0, nil, err
			}
			mapKeyToScore[k] = score
			score = 0
			k = ""
			continue
		}

		k = keyOrScore
	}
	return cursor, mapKeyToScore, nil
}

func execRedisZScore(key string, data interface{}) (int64, error) {
	ret, err := redis.Int64(ExecRedisCmd(0, "ZSCORE", key, data))
	if err != nil {
		return 0, err
	}
	return ret, nil
}
