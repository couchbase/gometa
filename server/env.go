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

package server

import (
	"bytes"
	json "encoding/json"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"net"
	"os"
	"strings"
)

type Env struct {
	hostUDPAddr     net.Addr
	hostTCPAddr     net.Addr
	hostRequestAddr net.Addr
	peerUDPAddr     []string
	peerTCPAddr     []string
}

type Node struct {
	ElectionAddr string
	MessageAddr  string
	RequestAddr  string
}

type Config struct {
	Host *Node
	Peer []*Node
}

var gEnv *Env

func NewEnv(config string) (err error) {
	gEnv = new(Env)

	if config == "" {
		return gEnv.initWithArgs()
	}

	return gEnv.initWithConfig(config)
}

func GetHostUDPAddr() string {
	return gEnv.hostUDPAddr.String()
}

func GetHostTCPAddr() string {
	return gEnv.hostTCPAddr.String()
}

func GetHostRequestAddr() string {
	return gEnv.hostRequestAddr.String()
}

func GetPeerUDPAddr() []string {
	return gEnv.peerUDPAddr
}

func GetPeerTCPAddr() []string {
	return gEnv.peerTCPAddr
}

func findMatchingPeerTCPAddr(updAddr string) string {
	for i := 0; i < len(gEnv.peerUDPAddr); i++ {
		if gEnv.peerUDPAddr[i] == updAddr {
			return gEnv.peerTCPAddr[i]
		}
	}
	return ""
}

func findMatchingPeerUDPAddr(tcpAddr string) string {
	for i := 0; i < len(gEnv.peerTCPAddr); i++ {
		if gEnv.peerTCPAddr[i] == tcpAddr {
			return gEnv.peerUDPAddr[i]
		}
	}
	return ""
}

func (e *Env) initWithConfig(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	_, err = buffer.ReadFrom(file)
	if err != nil {
		return err
	}

	var config Config
	err = json.Unmarshal(buffer.Bytes(), &config)
	if err != nil {
		return err
	}

	if e.hostUDPAddr, err = resolveAddr(common.ELECTION_TRANSPORT_TYPE, config.Host.ElectionAddr); err != nil {
		return err
	}
	log.Current.Debugf("Env.initWithConfig(): Host UDP Addr %s", e.hostUDPAddr.String())

	if e.hostTCPAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, config.Host.MessageAddr); err != nil {
		return err
	}
	log.Current.Debugf("Env.initWithConfig(): Host TCP Addr %s", e.hostTCPAddr.String())

	if e.hostRequestAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, config.Host.RequestAddr); err != nil {
		return err
	}
	log.Current.Debugf("Env.initWithConfig(): Host Request Addr %s", e.hostRequestAddr.String())

	e.peerUDPAddr = make([]string, 0, len(config.Peer))
	e.peerTCPAddr = make([]string, 0, len(config.Peer))

	for _, peer := range config.Peer {
		udpAddr, err := resolveAddr(common.ELECTION_TRANSPORT_TYPE, peer.ElectionAddr)
		if err != nil {
			return err
		}
		e.peerUDPAddr = append(e.peerUDPAddr, udpAddr.String())
		log.Current.Debugf("Env.initWithConfig(): Peer UDP Addr %s", udpAddr.String())

		tcpAddr, err := resolveAddr(common.MESSAGE_TRANSPORT_TYPE, peer.MessageAddr)
		if err != nil {
			return err
		}
		e.peerTCPAddr = append(e.peerTCPAddr, tcpAddr.String())
		log.Current.Debugf("Env.initWithConfig(): Peer TCP Addr %s", tcpAddr.String())
	}

	return nil
}

func (e *Env) initWithArgs() error {
	if len(os.Args) < 3 {
		return common.NewError(common.ARG_ERROR, "Missing command line argument")
	}

	err := e.resolveHostAddr()
	if err != nil {
		return err
	}

	if len(os.Args) >= 6 {
		e.resolvePeerAddr()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Env) resolveHostAddr() (err error) {

	e.hostUDPAddr, err = resolveAddr(common.ELECTION_TRANSPORT_TYPE, os.Args[1])
	if err != nil {
		return err
	}
	log.Current.Debugf("Env.resoleHostAddr(): Host UDP Addr %s", e.hostUDPAddr.String())

	e.hostTCPAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, os.Args[2])
	if err != nil {
		return err
	}
	log.Current.Debugf("Env.resolveHostAddr(): Host TCP Addr %s", e.hostTCPAddr.String())

	e.hostRequestAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, os.Args[3])
	if err != nil {
		return err
	}
	log.Current.Debugf("Env.resolveHostAddr(): Host Request Addr %s", e.hostRequestAddr.String())

	return nil
}

func (e *Env) resolvePeerAddr() error {
	args := os.Args[4:]
	e.peerUDPAddr = make([]string, 0, len(args))
	e.peerTCPAddr = make([]string, 0, len(args))

	for i := 0; i < len(args); {
		peer, err := resolveAddr(common.ELECTION_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err
		}
		e.peerUDPAddr = append(e.peerUDPAddr, peer.String())
		i++
		log.Current.Debugf("Env.resolvePeerAddr(): Peer UDP Addr %s", peer.String())

		peer, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err
		}
		e.peerTCPAddr = append(e.peerTCPAddr, peer.String())
		i++
		log.Current.Debugf("Env.resolvePeerAddr(): Peer TCP Addr %s", peer.String())
	}

	return nil
}

func resolveAddr(network string, addr string) (addrObj net.Addr, err error) {

	if strings.Contains(network, common.MESSAGE_TRANSPORT_TYPE) {
		addrObj, err = net.ResolveTCPAddr(network, addr)
	} else {
		addrObj, err = net.ResolveUDPAddr(network, addr)
	}

	if err != nil {
		return nil, err
	}

	return addrObj, nil
}
