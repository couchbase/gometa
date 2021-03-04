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
	"github.com/couchbase/gometa/common"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type observer struct {
	packets chan common.Packet
	head    common.Packet
	killch  chan bool
}

func NewObserver() *observer {

	return &observer{
		packets: make(chan common.Packet, common.MAX_PROPOSALS*20),
		head:    nil,
		killch:  make(chan bool)} // buffered - unblock sender
}

func (o *observer) close() {
	close(o.killch)
}

func (o *observer) send(msg common.Packet) {

	defer common.SafeRun("observer.Send()",
		func() {
			select {
			case o.packets <- msg: //no-op
			case <-o.killch:
				// if killch is closed, this is non-blocking.
				return
			}
		})
}

func (o *observer) getNext() common.Packet {
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
	if o.head != nil {
		return o.head
	}

	if len(o.packets) > 0 {
		o.head = <-o.packets
		return o.head
	}

	return nil
}
