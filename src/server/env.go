package server

import (
	"net"
)

func GetHostName() string {
	return "localhost:9998"
}

func GetPeers() []net.Addr {
	// TODO
	return nil
}