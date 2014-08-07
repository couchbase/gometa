package server

import (
	"net"
	"strings"
	"os"
	"common"
)

type Env struct {
	hostUDPAddr net.Addr 
	hostTCPAddr net.Addr 
	peerUDPAddr []string
	peerTCPAddr []string
}

var gEnv *Env

func NewEnv() (err error) {
	gEnv = new(Env)
	return gEnv.init()
}

func GetHostUDPAddr() string {
	return gEnv.hostUDPAddr.String()
}

func GetHostTCPAddr() string {
	return gEnv.hostTCPAddr.String()
}

func GetPeerUDPAddr() []string {
	return gEnv.peerUDPAddr
}

func GetPeerTCPAddr() []string {
	return gEnv.peerTCPAddr
}

func findMatchingPeerTCPAddr(updAddr string) string {
	for i :=0; i < len(gEnv.peerUDPAddr); i++ { 
		if gEnv.peerUDPAddr[i] == updAddr {
			return gEnv.peerTCPAddr[i]
		}
	}
	return "" 
}

func findMatchingPeerUDPAddr(tcpAddr string) string {
	for i :=0; i < len(gEnv.peerTCPAddr); i++ { 
		if gEnv.peerTCPAddr[i] == tcpAddr {
			return gEnv.peerUDPAddr[i]
		}
	}
	return "" 
}

func (e *Env) init() error {
	if len(os.Args) < 3 {
		return common.NewError(common.ARG_ERROR, "Missing command line argument")
	}
	
	err := e.resolveHostAddr()
	if err != nil {
		return err 
	}
	
	if len(os.Args) >= 5 {
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
		
	e.hostTCPAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, os.Args[2])
	if err != nil {
		return err	
	}
	
	return nil
}

func (e *Env) resolvePeerAddr() error {
	args := os.Args[3:]
	e.peerUDPAddr = make([]string, 0, len(args))
	e.peerTCPAddr = make([]string, 0, len(args))
	
	for i:=0; i < len(args); {
		peer, err := resolveAddr(common.ELECTION_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err	
		}
		e.peerUDPAddr = append(e.peerUDPAddr, peer.String())
		i++
		
		peer, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err	
		}
		e.peerTCPAddr = append(e.peerTCPAddr, peer.String())
		i++
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
