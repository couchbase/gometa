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

package protocol

import (
	"sync"

	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type observer struct {
	packets chan common.Packet
	head    common.Packet
	killch  chan bool
	mutex   *sync.RWMutex
}

func NewObserver() *observer {

	return &observer{
		packets: make(chan common.Packet, common.MAX_PROPOSALS*20),
		head:    nil,
		killch:  make(chan bool), // buffered - unblock sender
		mutex:   &sync.RWMutex{}}
}

func (o *observer) close() {
	close(o.killch)
}

func (o *observer) send(msg common.Packet) {

	defer common.SafeRun("observer.Send()",
		func() {
			needsResize := false
			func() {
				o.mutex.RLock()
				defer o.mutex.RUnlock()
				select {
				case o.packets <- msg: //no-op
				case <-o.killch:
					// if killch is closed, this is non-blocking.
					return
				default:
					needsResize = true
				}
			}()

			if needsResize {
				o.resizePacketsChan()

				o.mutex.RLock()
				defer o.mutex.RUnlock()

				select {
				case o.packets <- msg: //no-op
				case <-o.killch:
					// if killch is closed, this is non-blocking.
					return
				}
			}
		})
}

func (o *observer) getNext() common.Packet {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.head != nil {
		head := o.head
		o.head = nil
		return head
	}

	if len(o.packets) > 0 {
		packet := <-o.packets
		return packet
	}

	return nil
}

func (o *observer) peekFirst() common.Packet {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.head != nil {
		return o.head
	}

	if len(o.packets) > 0 {
		o.head = <-o.packets
		return o.head
	}

	return nil
}

func (o *observer) resizePacketsChan() {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if len(o.packets) == cap(o.packets) && cap(o.packets) < 1000*common.MAX_PROPOSALS {
		newPackets := make(chan common.Packet, min(2*cap(o.packets), 1000*common.MAX_PROPOSALS))
		for packet := range o.packets {
			newPackets <- packet
		}
		close(o.packets)
		o.packets = newPackets
		log.Current.Infof("Observer::Send() Doubled the size of packets channel, current cap: %v", cap(o.packets))
	}
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
