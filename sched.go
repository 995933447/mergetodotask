// Package mergetodotask 分布式合并消息式执行任务调度器
package mergetodotask

import (
	"fmt"
	"sync"
	"time"

	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/bucketsched"
	"github.com/995933447/runtimeutil"
	uuid "github.com/satori/go.uuid"
)

const (
	TtlDay = 3600 * 24 //1天缓存
)

type MergeTodoTaskSchedConf struct {
	Name                                      string
	TaskQueueSize                             int
	ConcurWorkerNum, BucketConcurWorkerMaxNum uint32
	OnListenMergeTodoTaskRegFromSlaveHdl      func()
	SendMergeTodoTaskRegEvtForMasterHdl       func(task MergeTodoTask)
	TaskAppIdSetRedisKey                      string                   // 可以把任务按分类应用id存储，方便按应用id查询挤压和管理任务，特别适合sass业务,可以是任何业务归属应用id,如商户id
	GenTaskSetRedisKeyHdl                     func(appId int32) string // 可以把任务按分类应用id存储，方便按应用id查询挤压和管理任务，特别适合sass业务,可以是任何业务归属应用id,如商户id
	DecodeMergeTodoTaskFromString             func(string) (MergeTodoTask, error)
	OnExecTask                                func(task MergeTodoTask) error
	ReloadRedoLogIntervalSec                  uint32
	RedisPool                                 RedisPool
	OnErr                                     func(err error)
}

func NewMergeTodoTaskSched(cfg *MergeTodoTaskSchedConf) (*MergeTodoTaskSched, error) {
	ExecRedisCmd = execRedisCmd(cfg.RedisPool)

	e, err := factory.NewAutoElection(factory.ElectDriverDistribMuRedis, factory.NewDistribMuRedisCfg(cfg.RedisPool, cfg.Name, uuid.NewV4().String()))
	if err != nil {
		return nil, err
	}

	e.LoopInElectV2(nil, cfg.OnErr)

	if cfg.BucketConcurWorkerMaxNum == 0 {
		cfg.BucketConcurWorkerMaxNum = 1
	}
	// 总共并行线程ConcurWorkerNum个，但是根据根据bucket进行资源限制调度，一个bucket同时只存在BucketConcurWorkerMaxNum个同步任务运行,
	// 队列会根据bucket交替执行任务，保证调度公平，不会有bucket进入饥饿等待状态
	queue := bucketsched.NewBucketQueueWithBucketConcur(cfg.Name, cfg.ConcurWorkerNum, cfg.BucketConcurWorkerMaxNum)
	if cfg.TaskQueueSize > 0 {
		queue.SetSize(cfg.TaskQueueSize)
	}
	sched := &MergeTodoTaskSched{
		queue:              queue,
		elect:              e,
		cfg:                cfg,
		mapAppIdToTaskStrs: make(map[int32]map[string]interface{}),
	}

	runtimeutil.Go(func() {
		queue.Run()
	})

	// 监听从节点广播的任务并注册，只有master节点负责调度执行
	sched.initMergeTodoTaskFromSlaveListener()
	// 初始化重试任务加载
	sched.initRedoMergeTodoTaskLoader()
	// 初始化无效任务清理
	sched.initInvalidTxnLogClearer()

	return sched, nil
}

type MergeTodoTaskSched struct {
	queue               *bucketsched.BucketQueue
	elect               autoelectv2.AutoElection
	cfg                 *MergeTodoTaskSchedConf
	lastLoadRedoLogTime time.Time
	mapAppIdToTaskStrs  map[int32]map[string]interface{}
	mu                  sync.RWMutex
}

func (s *MergeTodoTaskSched) OnErr(err error) {
	if s.cfg.OnErr != nil {
		s.cfg.OnErr(runtimeutil.NewStackErrWithSkip(2, err))
	}
}

func (s *MergeTodoTaskSched) IsMaster() bool {
	return s.elect.IsMaster()
}

func (s *MergeTodoTaskSched) initMergeTodoTaskFromSlaveListener() {
	if s.cfg.OnListenMergeTodoTaskRegFromSlaveHdl != nil {
		s.cfg.OnListenMergeTodoTaskRegFromSlaveHdl()
	}
}

// 初始化无效日志清理器
func (s *MergeTodoTaskSched) initInvalidTxnLogClearer() {
	runtimeutil.Go(func() {
		for {
			if !s.elect.IsMaster() {
				time.Sleep(time.Second)
				continue
			}

			_, err := s.clearInvalidTxnLog()
			if err != nil {
				s.OnErr(fmt.Errorf("clear invalid txn log failed,err:%v", err))
			}

			time.Sleep(time.Minute)
		}
	})
}

// 重载上次进程退出前未完成的任务
func (s *MergeTodoTaskSched) initRedoMergeTodoTaskLoader() {
	reloadIntervalSec := time.Duration(1)
	if s.cfg.ReloadRedoLogIntervalSec > 0 {
		reloadIntervalSec = time.Duration(s.cfg.ReloadRedoLogIntervalSec)
	}
	go func() {
		for {
			if !s.elect.IsMaster() {
				time.Sleep(time.Second)
				continue
			}

			// 本地没有任务了，去窃取一些回来调度执行，也可以避免其他进程异常退出了，异常进程的任务没有执行。
			if err := s.stealMergeTodoTasks(); err != nil {
				s.OnErr(fmt.Errorf("steal tasks failed,err:%v", err))
			}

			time.Sleep(time.Second * reloadIntervalSec)
		}
	}()
}

// 本进程没有任务了去窃取一些任务，同时可以避免其他进程有异常退出的情况导致任务没有进行
func (s *MergeTodoTaskSched) stealMergeTodoTasks() error {
	if s.queue.Size() > 0 {
		return nil
	}
	err := s.loadAllRedoLog()
	if err != nil {
		s.OnErr(fmt.Errorf("load all redo log failed,err:%v", err))
		return err
	}
	return nil
}

// 加载redo log
func (s *MergeTodoTaskSched) loadAllRedoLog() error {
	var err error
	// 加载需要重试的任务
	appIds, err := execRedisSMembers(s.cfg.TaskAppIdSetRedisKey)
	if err != nil {
		s.OnErr(fmt.Errorf("exec redis SMEMBER failed,err:%v", err))
		return err
	}
	for _, appId := range appIds {
		var cursor int64
		redoLogKey := s.getTxnRedoRedisKey(int32(appId))
		if time.Since(s.lastLoadRedoLogTime) > time.Minute {
			s.lastLoadRedoLogTime = time.Now()
			for {
				mapTaskStrToRedoScore := map[string]int64{}
				cursor, mapTaskStrToRedoScore, err = execZScanWithScore(redoLogKey, cursor, 1000)
				if err != nil {
					s.OnErr(fmt.Errorf("redisDeal.RedisDoZscan failed, key:%s, cursor:%d, err:%v", redoLogKey, cursor, err))
					break
				}

				for taskStr, score := range mapTaskStrToRedoScore {
					if score <= 0 {
						continue
					}

					task, err := s.cfg.DecodeMergeTodoTaskFromString(taskStr)
					if err != nil {
						s.OnErr(fmt.Errorf("decode task from redis failed, task:%s, err:%v", taskStr, err))
						return err
					}

					err = s.writeOpLog(int32(appId), task)
					if err != nil {
						s.OnErr(fmt.Errorf("write op log failed, task:%s, err:%v", taskStr, err))
						return err
					}
				}

				if cursor == 0 {
					break
				}

				time.Sleep(time.Millisecond * 100)
			}
		}

		opLogKey := s.cfg.GenTaskSetRedisKeyHdl(int32(appId))
		for {
			mapTaskStrToRedoScore := map[string]int64{}
			cursor, mapTaskStrToRedoScore, err = execZScanWithScore(opLogKey, cursor, 1000)
			if err != nil {
				s.OnErr(fmt.Errorf("exec redis ZSCAN failed, key:%s, cursor:%d, err:%v", opLogKey, cursor, err))
				break
			}

			for taskStr, score := range mapTaskStrToRedoScore {
				if score <= 0 {
					continue
				}

				task, err := s.cfg.DecodeMergeTodoTaskFromString(taskStr)
				if err != nil {
					s.OnErr(fmt.Errorf("decode task from redis failed, task:%s, err:%v", taskStr, err))
					return err
				}

				err = s.enqueueMergeTodoTask(int32(appId), task)
				if err != nil {
					s.OnErr(fmt.Errorf("enqueue task failed, task:%s, err:%v", taskStr, err))
					return err
				}
			}

			if cursor == 0 {
				break
			}

			time.Sleep(time.Millisecond * 100)
		}
	}

	return nil
}

// 清理已经完成的任务，把redo日志移除
func (s *MergeTodoTaskSched) clearInvalidTxnLog() (bool, error) {
	appIds, err := execRedisSMembers(s.cfg.TaskAppIdSetRedisKey)
	if err != nil {
		s.OnErr(fmt.Errorf("exec redis SMEMBER failed,err:%v", err))
		return false, err
	}
	for _, appId := range appIds {
		err := execZRemRangeByScore(s.cfg.GenTaskSetRedisKeyHdl(int32(appId)), -9999, 0)
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis ZREMRANGEBYSCORE err:%v", err))
			return false, err
		}

		err = execZRemRangeByScore(s.getTxnRedoRedisKey(int32(appId)), -9999, 0)
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis ZREMRANGEBYSCORE err:%v", err))
			return false, err
		}
	}

	return true, nil
}

func (s *MergeTodoTaskSched) getTxnRedoRedisKey(appId int32) string {
	redoLogKey := s.cfg.GenTaskSetRedisKeyHdl(appId)
	redoLogKey += ".redo"
	return redoLogKey
}

func (s *MergeTodoTaskSched) cancelRedoLog(appId int32, task MergeTodoTask) error {
	err := execRedisZIncrBy(s.getTxnRedoRedisKey(appId), -1, task.String(), TtlDay)
	if err != nil {
		s.OnErr(fmt.Errorf("exec redis ZINCRBy err:%v", err))
		return err
	}
	return nil
}

func (s *MergeTodoTaskSched) writeRedoLog(appId int32, task MergeTodoTask) error {
	errGrp := runtimeutil.NewErrGrp()

	errGrp.Go(func() error {
		err := execRedisSAdd(s.cfg.TaskAppIdSetRedisKey, fmt.Sprintf("%d", appId), TtlDay)
		if err != nil {
			return err
		}
		return nil
	})

	errGrp.Go(func() error {
		err := execRedisZIncrBy(s.getTxnRedoRedisKey(appId), 1, task.String(), TtlDay)
		if err != nil {
			return err
		}
		return nil
	})

	if err := errGrp.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *MergeTodoTaskSched) writeOpLog(appId int32, task MergeTodoTask) error {
	errGrp := runtimeutil.NewErrGrp()

	errGrp.Go(func() error {
		err := s.cancelRedoLog(appId, task)
		if err != nil {
			s.OnErr(fmt.Errorf("cancel redo log failed, err:%v", err))
			return err
		}
		return nil
	})

	errGrp.Go(func() error {
		err := execRedisSAdd(s.cfg.TaskAppIdSetRedisKey, fmt.Sprintf("%d", appId), TtlDay)
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis SADD err:%v", err))
			return err
		}
		return nil
	})

	errGrp.Go(func() error {
		err := execRedisZIncrBy(s.cfg.GenTaskSetRedisKeyHdl(appId), 1, task.String(), TtlDay)
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis ZINCRBy err:%v", err))
			return err
		}
		return nil
	})

	if err := errGrp.Wait(); err != nil {
		s.OnErr(fmt.Errorf("write op log failed, err:%v", err))
		return err
	}

	return nil
}

// PreWriteRedoLog 预写redo日志。用于避免进程准备任务任务数据时还没来得及注册任务异常退出了,下次新的master加载重做事务日志保证任务完成
func (s *MergeTodoTaskSched) PreWriteRedoLog(appId int32, task MergeTodoTask) error {
	if err := s.writeRedoLog(appId, task); err != nil {
		s.OnErr(fmt.Errorf("write redo log failed, err:%v", err))
		return err
	}

	return nil
}

type MergeTodoTask interface {
	GetBucketId() int64
	String() string
}

func (s *MergeTodoTaskSched) checkRepeatOrMemMergeTodoTask(appId int32, MergeTodoTask MergeTodoTask) bool {
	s.mu.RLock()
	taskStrs, ok := s.mapAppIdToTaskStrs[appId]
	if ok {
		if _, ok = taskStrs[MergeTodoTask.String()]; ok {
			s.mu.RUnlock()
			return true
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	taskStrs, ok = s.mapAppIdToTaskStrs[appId]
	if ok {
		if _, ok = taskStrs[MergeTodoTask.String()]; ok {
			return true
		}
	} else {
		taskStrs = make(map[string]interface{})
		s.mapAppIdToTaskStrs[appId] = taskStrs
	}
	taskStrs[MergeTodoTask.String()] = struct{}{}

	return false
}

// RegMergeTodoTask 注册任务
func (s *MergeTodoTaskSched) RegMergeTodoTask(appId int32, MergeTodoTask MergeTodoTask) error {
	if !s.IsMaster() {
		if s.cfg.SendMergeTodoTaskRegEvtForMasterHdl != nil {
			s.cfg.SendMergeTodoTaskRegEvtForMasterHdl(MergeTodoTask)
			return nil
		}
	}

	if err := s.writeOpLog(appId, MergeTodoTask); err != nil {
		s.OnErr(fmt.Errorf("write op log failed, err:%v", err))
		return err
	}

	if !s.IsMaster() {
		return nil
	}

	if err := s.enqueueMergeTodoTask(appId, MergeTodoTask); err != nil {
		s.OnErr(fmt.Errorf("enqueue sync task failed, err:%v", err))
		return err
	}

	return nil
}

func (s *MergeTodoTaskSched) enqueueMergeTodoTask(appId int32, MergeTodoTask MergeTodoTask) error {
	if s.checkRepeatOrMemMergeTodoTask(appId, MergeTodoTask) {
		return nil
	}

	task := bucketsched.NewTask(MergeTodoTask.GetBucketId(), fmt.Sprintf("MergeTodoTask.%s", MergeTodoTask.String()), func(task *bucketsched.Task) error {
		s.mu.Lock()
		taskStrs, ok := s.mapAppIdToTaskStrs[appId]
		if ok {
			delete(taskStrs, MergeTodoTask.String())
		}
		s.mu.Unlock()

		taskZetRedisKey := s.cfg.GenTaskSetRedisKeyHdl(appId)
		updateCnt, err := execRedisZScore(taskZetRedisKey, MergeTodoTask.String())
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis ZSCORE err:%v", err))
			return err
		}

		if updateCnt <= 0 {
			return nil
		}

		if err := s.cfg.OnExecTask(MergeTodoTask); err != nil {
			s.checkRepeatOrMemMergeTodoTask(appId, MergeTodoTask)
			return err
		}

		err = execRedisZIncrBy(taskZetRedisKey, -updateCnt, MergeTodoTask.String(), TtlDay)
		if err != nil {
			s.OnErr(fmt.Errorf("exec redis ZINCRBy err:%v", err))
		}

		return nil
	}, 0, 10)

	if err := s.queue.Reg(task); err != nil {
		s.OnErr(fmt.Errorf("queue reg err:%v", err))
		return err
	}

	return nil
}

// TxnRegMergeTodoTask 便捷地开启一个注册任务的事务（包含预写重做日志，注册任务步骤）
func (s *MergeTodoTaskSched) TxnRegMergeTodoTask(appId int32, MergeTodoTask MergeTodoTask, prepareTxnFn func() error) error {
	if err := s.PreWriteRedoLog(appId, MergeTodoTask); err != nil {
		s.OnErr(fmt.Errorf("write redo log failed, err:%v", err))
		return err
	}

	if err := prepareTxnFn(); err != nil {
		s.OnErr(fmt.Errorf("prepare txn fail, err:%v", err))
		return err
	}

	if err := s.RegMergeTodoTask(appId, MergeTodoTask); err != nil {
		s.OnErr(fmt.Errorf("reg sync task fail, err:%v", err))
		return err
	}

	return nil
}
