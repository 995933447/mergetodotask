package mergetodotask

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"testing"
	"time"
)

func TestSched(t *testing.T) {
	sched, err := NewMergeTodoTaskSched(&MergeTodoTaskSchedConf{
		Name:                     "distMergeTodoTaskSchedTest",
		BucketConcurWorkerMaxNum: 1,
		ConcurWorkerNum:          32,
		TaskAppIdSetRedisKey:     "testDistMergeTodoTaskAppIdSet",
		GenTaskSetRedisKeyHdl: func(appId int32) string {
			return fmt.Sprintf("testDistMergeTodoTaskAppId:%d", appId)
		},
		DecodeMergeTodoTaskFromString: func(s string) (MergeTodoTask, error) {
			uid, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return nil, err
			}
			return &TestDistMergeTodoTask{
				Uid: uid,
				Ts:  time.Now().Unix(),
			}, nil
		},
		OnExecTask: func(task MergeTodoTask) error {
			fmt.Printf("started %d %+v %+v\n", task.GetBucketId(), task, task.(*TestDistMergeTodoTask).Ts)
			time.Sleep(time.Second)
			return nil
		},
		OnErr: func(err error) {
			fmt.Printf("err:%v\n", err)
		},
		RedisPool: GetRedisPool(),
	})
	if err != nil {
		panic(err)
	}

	for i := uint64(1); i < 15; i++ {
		err := sched.TxnRegMergeTodoTask(0, &TestDistMergeTodoTask{
			Uid: i,
		}, func() error {
			time.Sleep(time.Second)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}

	select {}
}

type TestDistMergeTodoTask struct {
	Uid uint64
	Ts  int64
}

func (t *TestDistMergeTodoTask) GetBucketId() int64 {
	return int64(t.Uid) % 100
}

func (t *TestDistMergeTodoTask) String() string {
	return fmt.Sprintf("%d", t.Uid)
}

func GetRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:         2,
		MaxActive:       0, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout:     time.Minute * 3,
		MaxConnLifetime: time.Minute * 10,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "10.5.37.106:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			return nil
		},
	}
}
