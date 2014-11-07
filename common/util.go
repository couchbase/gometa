// @author Couchbase <info@couchbase.com>
// @copyright 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"log"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Utility
/////////////////////////////////////////////////////////////////////////////

type FuncToRun func()

func SafeRun(funcName string, f FuncToRun) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in %s() : %s\n", funcName, r)
		}
	}()

	f()
}

type CompareResult byte

const (
	EQUAL CompareResult = iota
	GREATER
	LESSER
	MORE_RECENT
	LESS_RECENT
)

type Cleanup struct {
	canceled bool
	f        func()
}

func NewCleanup(f func()) *Cleanup {
	return &Cleanup{
		canceled: false,
		f:        f,
	}
}

func (c *Cleanup) Run() {
	if !c.canceled {
		c.f()
	}
}

func (c *Cleanup) Cancel() {
	c.canceled = true
}

type BackoffTimer struct {
	timer *time.Timer

	duration    time.Duration
	maxDuration time.Duration
	factor      int

	currentDuration time.Duration
}

func NewBackoffTimer(duration time.Duration,
	maxDuration time.Duration, factor int) *BackoffTimer {

	return &BackoffTimer{
		timer: time.NewTimer(duration),

		duration:    duration,
		maxDuration: maxDuration,
		factor:      factor,

		currentDuration: duration,
	}
}

func (t *BackoffTimer) GetChannel() <-chan time.Time {
	return t.timer.C
}

func (t *BackoffTimer) Stop() bool {
	return t.timer.Stop()
}

func (t *BackoffTimer) Reset() {
	t.currentDuration = t.duration
	t.timer.Reset(t.currentDuration)
}

func (t *BackoffTimer) Backoff() {
	t.currentDuration *= time.Duration(t.factor)
	if t.currentDuration > t.maxDuration {
		t.currentDuration = t.maxDuration
	}

	t.timer.Reset(t.currentDuration)
}

type ResettableTimer struct {
	d time.Duration

	*time.Timer
}

func (t *ResettableTimer) Reset() {
	t.Timer.Reset(t.d)
}

func NewResettableTimer(d time.Duration) *ResettableTimer {
	return &ResettableTimer{
		d:     d,
		Timer: time.NewTimer(d),
	}
}

func NewStoppedResettableTimer(d time.Duration) *ResettableTimer {
	timer := NewResettableTimer(d)
	timer.Stop()
	select {
	case <-timer.C:
	default:
	}

	return timer
}
