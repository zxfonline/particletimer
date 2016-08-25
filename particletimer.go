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
	"github.com/zxfonline/timefix"
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

var (
	logger     *golog.Logger                       = golog.New("ParticleTimer")
	_eventlist [TIMER_LEVEL]map[int64]_timer_event // 事件列表

	_eventqueue      map[int64]_timer_event // 事件添加队列
	_eventqueue_lock sync.Mutex

	_deleted_timers      map[int64]bool
	_deleted_timers_lock sync.Mutex

	_timer_id int64 // 内部事件编号

	stopD chanutil.DoneChan
	state int32
)

func init() {
	for k := range _eventlist {
		_eventlist[k] = make(map[int64]_timer_event)
	}
	_eventqueue = make(map[int64]_timer_event)
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
	return timefix.MillisUTCTime()
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
			if len(_eventqueue) > 0 {
				// 处理排队
				// 最小的时间间隔，处理为1ms
				_eventqueue_lock.Lock()
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
				_eventqueue = make(map[int64]_timer_event)
				_eventqueue_lock.Unlock()
			}
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
				func() {
					defer func() {
						if err := recover(); err != nil {
							logger.Warnf("TRIGGER err:%+v channel len:%d", err, len(v.ch))
						}
					}()
					if checkValidTimer(v.Id) {
						v.ch <- v.Id
					}
				}()
			} else { // 移动到前一个更短间距的LIST
				_eventlist[level-1][k] = v
			}
			delete(list, k)
		}
	}
}

// 添加一个定时事件，timeout为到期的修正UTC时间 毫秒  id 是调用者定义的编号(请使用唯一id生成器生成), 事件发生时，会把id发送到ch
func Add(id int64, timeout int64, ch chan int64) {
	if atomic.LoadInt32(&state) != 1 {
		panic(errors.New("particle timer closed"))
	}
	timer_id := atomic.AddInt64(&_timer_id, 1)
	event := _timer_event{Id: id, ch: ch, Timeout: timeout}
	_eventqueue_lock.Lock()
	_eventqueue[timer_id] = event
	_eventqueue_lock.Unlock()
}

//删除指定id的定时事件
func Del(id int64) {
	_deleted_timers_lock.Lock()
	_deleted_timers[id] = true
	_deleted_timers_lock.Unlock()
}

func checkValidTimer(id int64) bool {
	flag := true
	_deleted_timers_lock.Lock()
	if deleted, _ := _deleted_timers[id]; deleted {
		flag = false
	}
	delete(_deleted_timers, id)
	_deleted_timers_lock.Unlock()
	return flag
}
