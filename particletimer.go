// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package particletimer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zxfonline/chanutil"
	"github.com/zxfonline/golog"
)

type _timer_event struct {
	Id      int64      // 自定义id
	Timeout int64      // 到期的修正UTC时间 毫秒
	ch      chan int64 // 发送通道
}

const (
	TIMER_LEVEL = uint(48) // 时间段最大分级，最大时间段为 2^TIMER_LEVEL
	TICK_TIME   = 10       // 10 ms
)

var logger *golog.Logger = golog.New("ParticleTimer")
var (
	_eventlist [TIMER_LEVEL]map[int64]*_timer_event // 分级事件列表

	_eventqueue      map[int64]*_timer_event // 新增事件添加队列
	_alleventqueue   map[int64]*_timer_event // 所有事件添加队列
	_eventqueue_lock sync.Mutex

	_deleted_timers map[int64]bool

	_timer_id int64 // 内部事件编号

	stopD chanutil.DoneChan
	state int32
)

func init() {
	for k := range _eventlist {
		_eventlist[k] = make(map[int64]*_timer_event)
	}
	_eventqueue = make(map[int64]*_timer_event)
	_alleventqueue = make(map[int64]*_timer_event)
	_deleted_timers = make(map[int64]bool)
	stopD = chanutil.NewDoneChan()
	go _timer()
}
func Closed() bool {
	return stopD.R().Done()
}

func Close() {
	if !atomic.CompareAndSwapInt32(&state, 1, 2) {
		return
	}
	stopD.SetDone()
}

//当前修正 UTC时间 毫秒
func Current() int64 {
	return time.Now().UnixNano() / 1e6
}

// 定时器 根据程序启动后经过的秒数计数
func _timer() {
	if !atomic.CompareAndSwapInt32(&state, 0, 1) {
		return
	}
	timer_count := uint64(0)
	last := Current()
	for q := false; !q; {
		select {
		case <-stopD:
			q = true
		default:
			time.Sleep(TICK_TIME * time.Millisecond)
			_eventqueue_lock.Lock()
			if len(_eventqueue) > 0 {
				// 处理排队
				// 最小的时间间隔，处理为1ms
				for k, v := range _eventqueue {
					// 处理微小间隔
					diff := v.Timeout - Current()
					if diff <= 0 {
						diff = 1
					}
					// 发到合适的时间段
					for i := TIMER_LEVEL - 1; i >= 0; i-- {
						if diff >= 1<<i {
							_eventlist[i][k] = v
							break
						}
					}
				}
				_eventqueue = make(map[int64]*_timer_event)
			}
			_eventqueue_lock.Unlock()
			// 检查事件触发
			// 累计距离上一次触发的秒数,并逐秒触发
			// 如果校正了系统时间，时间前移，nmsecs为负数的时候，last的值不应该变动，否则会出现秒数的重复计数
			now := Current()
			nmsecs := now - last
			if nmsecs <= 0 {
				continue
			} else {
				last = now
			}
			for c := int64(0); c < nmsecs; c++ {
				timer_count++
				for i := TIMER_LEVEL - 1; i > 0; i-- {
					mask := (uint64(1) << i) - 1
					if timer_count&mask == 0 {
						_trigger(i)
					}
				}
				_trigger(0)
			}
		}
	}
}

//单级触发
func _trigger(level uint) {
	now := Current()
	list := _eventlist[level]

	for k, v := range list {
		if v.Timeout-now < 1<<level {
			if level == 0 {
				func(tk int64, tv *_timer_event) {
					defer func() {
						if err := recover(); err != nil {
							logger.Errorf("TRIGGER err:%+v channel len:%d\n", err, len(tv.ch))
						}
					}()

					_eventqueue_lock.Lock()
					defer _eventqueue_lock.Unlock()
					delete(_alleventqueue, tv.Id)
					if checkValidTimer(tv.Id) {
						tv.ch <- tv.Id
						// logger.Debugf("trigger timer ok, timerid:%d,ptid:%d,timeout:%d", tv.Id, tk, tv.Timeout)
					} else {
						// logger.Warnf("trigger timer fail, timerid:%d,ptid:%d,timeout:%d", tv.Id, tk, tv.Timeout)
					}
				}(k, v)
			} else { // 移动到前一个更短间距的LIST
				_eventlist[level-1][k] = v
			}
			delete(list, k)
		}
	}
}

// 添加一个定时事件，timeout为到期的修正UTC时间 毫秒  id 是调用者定义的编号(请使用唯一id生成器生成), 事件发生时，会把id发送到ch
func Add(id int64, timeout int64, ch chan int64) int64 {
	if atomic.LoadInt32(&state) != 1 {
		panic(errors.New("particle timer closed"))
	}
	_eventqueue_lock.Lock()
	defer _eventqueue_lock.Unlock()
	_timer_id++
	event := &_timer_event{Id: id, ch: ch, Timeout: timeout}
	_eventqueue[_timer_id] = event
	_alleventqueue[id] = event
	// logger.Debugf("add timer ok, timerid:%d,ptid:%d,timeout:%d", id, _timer_id, timeout)
	return _timer_id
}

func ParticleTimerExist(id int64) bool {
	_eventqueue_lock.Lock()
	defer _eventqueue_lock.Unlock()
	_, exists := _alleventqueue[id]
	return exists
}

func ParticleTimerInfo(ptid int64) (bool, int) {
	_eventqueue_lock.Lock()
	defer _eventqueue_lock.Unlock()
	for level, mp := range _eventlist {
		for k := range mp {
			if k == ptid {
				return true, level
			}
		}
	}
	return false, 0
}

//删除指定id的定时事件
func Del(id int64) {
	_eventqueue_lock.Lock()
	defer _eventqueue_lock.Unlock()
	if _, exists := _alleventqueue[id]; exists {
		_deleted_timers[id] = true
	}
}

func checkValidTimer(id int64) bool {
	flag := true
	if _, deleted := _deleted_timers[id]; deleted {
		flag = false
		delete(_deleted_timers, id)
	}
	return flag
}
