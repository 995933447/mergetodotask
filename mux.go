package mergetodotask

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrRoutedMergeTodoTaskDecodeFailed = errors.New("decode routed task failed")
	ErrRouteCmdNotFound                = errors.New("route cmd not found")
)

func NewMergeTodoTaskSchedMux(cfg *MergeTodoTaskSchedMuxConf) (*MergeTodoTaskSchedMux, error) {
	sched, err := NewMergeTodoTaskSched(&MergeTodoTaskSchedConf{
		Name:                                 cfg.Name,
		TaskQueueSize:                        cfg.TaskQueueSize,
		ConcurWorkerNum:                      cfg.ConcurWorkerNum,
		BucketConcurWorkerMaxNum:             cfg.BucketConcurWorkerMaxNum,
		OnListenMergeTodoTaskRegFromSlaveHdl: cfg.OnListenMergeTodoTaskRegFromSlaveHdl,
		TaskAppIdSetRedisKey:                 cfg.TaskAppIdSetRedisKey,
		GenTaskSetRedisKeyHdl:                cfg.GenTaskSetRedisKeyHdl,
		ReloadRedoLogIntervalSec:             cfg.ReloadRedoLogIntervalSec,
		DecodeMergeTodoTaskFromString:        DecodeRoutedMergeTodoTaskWrapperFunc(cfg),
		OnExecTask:                           OnExecRoutedMergeTodoTaskFunc(cfg),
		OnErr:                                cfg.OnErr,
	})
	if err != nil {
		return nil, err
	}
	return &MergeTodoTaskSchedMux{
		sched: sched,
	}, nil
}

// MergeTodoTaskSchedMux 路由式注册处理器的任务调度器
type MergeTodoTaskSchedMux struct {
	sched *MergeTodoTaskSched
}

// PreWriteRedoLog 预写redo日志
func (s *MergeTodoTaskSchedMux) PreWriteRedoLog(appId int32, MergeTodoTask RoutedMergeTodoTask) error {
	err := s.sched.PreWriteRedoLog(appId, &RoutedMergeTodoTaskWrapper{
		RouteCmd:            MergeTodoTask.GetRouteCmd(),
		RoutedMergeTodoTask: MergeTodoTask,
	})
	if err != nil {
		return err
	}
	return nil
}

// RegMergeTodoTask 注册任务
func (s *MergeTodoTaskSchedMux) RegMergeTodoTask(appId int32, MergeTodoTask RoutedMergeTodoTask) error {
	err := s.sched.RegMergeTodoTask(appId, &RoutedMergeTodoTaskWrapper{
		RouteCmd:            MergeTodoTask.GetRouteCmd(),
		RoutedMergeTodoTask: MergeTodoTask,
	})
	if err != nil {
		return err
	}
	return nil
}

// TxnRegMergeTodoTask 便捷地开启一个注册任务的事务（包含预写重做日志，注册任务步骤）
func (s *MergeTodoTaskSchedMux) TxnRegMergeTodoTask(appId int32, MergeTodoTask RoutedMergeTodoTask, prepareTxnFn func() error) error {
	err := s.sched.TxnRegMergeTodoTask(appId, &RoutedMergeTodoTaskWrapper{
		RouteCmd:            MergeTodoTask.GetRouteCmd(),
		RoutedMergeTodoTask: MergeTodoTask,
	}, prepareTxnFn)
	if err != nil {
		return err
	}
	return nil
}

func OnExecRoutedMergeTodoTaskFunc(cfg *MergeTodoTaskSchedMuxConf) func(task MergeTodoTask) error {
	return func(task MergeTodoTask) error {
		routedTaskWrapper := task.(*RoutedMergeTodoTaskWrapper)

		route, ok := cfg.GetRoute(routedTaskWrapper.RouteCmd)
		if !ok {
			return ErrRouteCmdNotFound
		}

		if err := route.OnExecRoutedMergeTodoTaskFunc(routedTaskWrapper.RoutedMergeTodoTask); err != nil {
			return err
		}

		return nil
	}
}

type MergeTodoTaskSchedMuxConf struct {
	Name                                      string
	TaskQueueSize                             int
	ConcurWorkerNum, BucketConcurWorkerMaxNum uint32
	OnListenMergeTodoTaskRegFromSlaveHdl      func()
	TaskAppIdSetRedisKey                      string // 可以把任务按分类应用id存储，方便按应用id查询挤压和管理任务，特别适合sass业务,可以是任何业务归属应用id,如商户id
	GenTaskSetRedisKeyHdl                     func(appId int32) string
	ReloadRedoLogIntervalSec                  uint32
	Routes                                    map[RouteCmd]*Route
	OnErr                                     func(error)
	mu                                        sync.RWMutex
}

func (c *MergeTodoTaskSchedMuxConf) AddRoutes(routes map[RouteCmd]*Route) error {
	for cmd, route := range routes {
		if err := c.AddRoute(cmd, route); err != nil {
			return err
		}
	}
	return nil
}

func (c *MergeTodoTaskSchedMuxConf) AddRoute(cmd RouteCmd, route *Route) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Routes == nil {
		c.Routes = map[RouteCmd]*Route{}
	}
	if _, ok := c.Routes[cmd]; ok {
		return fmt.Errorf("route cmd:%d duplicated", cmd)
	}
	c.Routes[cmd] = route
	return nil
}

func (c *MergeTodoTaskSchedMuxConf) GetRoute(cmd RouteCmd) (*Route, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.Routes == nil {
		return nil, false
	}
	route, ok := c.Routes[cmd]
	return route, ok
}

func DecodeRoutedMergeTodoTaskWrapperFunc(cfg *MergeTodoTaskSchedMuxConf) func(s string) (MergeTodoTask, error) {
	return func(s string) (MergeTodoTask, error) {
		routedTaskComponent := strings.SplitN(s, "@", 2)
		if len(routedTaskComponent) != 2 {
			return nil, ErrRoutedMergeTodoTaskDecodeFailed
		}

		routeCmdStr := routedTaskComponent[0]
		routeCmdInt64, err := strconv.ParseInt(routeCmdStr, 10, 32)
		if err != nil {
			return nil, err
		}

		routeCmd := RouteCmd(routeCmdInt64)

		route, ok := cfg.GetRoute(routeCmd)
		if !ok {
			return nil, ErrRoutedMergeTodoTaskDecodeFailed
		}

		routedMergeTodoTask, err := route.DecodeRoutedMergeTodoTaskFunc(routedTaskComponent[1])
		if err != nil {
			return nil, err
		}

		return &RoutedMergeTodoTaskWrapper{
			RouteCmd:            routeCmd,
			RoutedMergeTodoTask: routedMergeTodoTask,
		}, nil
	}
}

var _ MergeTodoTask = (*RoutedMergeTodoTaskWrapper)(nil)

type Route struct {
	DecodeRoutedMergeTodoTaskFunc func(s string) (RoutedMergeTodoTask, error)
	OnExecRoutedMergeTodoTaskFunc func(task RoutedMergeTodoTask) error
}

type RouteCmd int32

type RoutedMergeTodoTaskWrapper struct {
	RouteCmd            RouteCmd
	RoutedMergeTodoTask RoutedMergeTodoTask
}

func (w *RoutedMergeTodoTaskWrapper) GetBucketId() int64 {
	return w.RoutedMergeTodoTask.GetBucketId()
}

func (w *RoutedMergeTodoTaskWrapper) String() string {
	return fmt.Sprintf("%d@%s", w.RouteCmd, w.RoutedMergeTodoTask.String())
}

type RoutedMergeTodoTask interface {
	MergeTodoTask
	GetRouteCmd() RouteCmd
}
