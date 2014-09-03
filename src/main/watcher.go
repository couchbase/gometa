package main

import (
	"fmt"
	"github.com/jliang00/gometa/src/server"
	"github.com/jliang00/gometa/src/message"
	"github.com/jliang00/gometa/src/protocol"
	"github.com/jliang00/gometa/src/common"
	repo "github.com/jliang00/gometa/src/repository"
)

type fakeServer struct {
	repo      		*repo.Repository
	log       		*repo.CommitLog
	srvConfig 		*repo.ServerConfig
	factory   		protocol.MsgFactory
	handler   		*server.ServerAction	
	killch			chan bool
	status 			protocol.PeerStatus
}

func test() {
}

func runWatcher(path string) {

	if path == "" {
		fmt.Printf("Missing configuration")
		return
		
	}

	// setup env	
	if err := server.NewEnv(path); err != nil {
		return 
	}
	
	// create a fake server
	fs := new(fakeServer)
	fs.bootstrap()
	
	go protocol.RunWatcherServer(	
		server.GetHostUDPAddr(),
		server.GetPeerUDPAddr(),
		server.GetPeerTCPAddr(),
		fs.handler,
		fs.factory,		
		fs.killch)
		
	runConsole(fs)
}

func runConsole(fs *fakeServer) {

	for {
		// read command from console 
		var key string
		
		fmt.Printf("Enter Key to Retrieve\n")
		_, err := fmt.Scanf("%s", &key)
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			continue
		}
		
		value, err := fs.handler.Get(key)		
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			continue
		}	
		
	   	if value != nil { 
			fmt.Printf("Result = %s \n", string(value))
		}  else {
			fmt.Printf("Result not found\n")
		}
	}
}

func (s *fakeServer) bootstrap() (err error) {

	// Initialize repository service	
	s.repo, err = repo.OpenRepository()
	if err != nil {
		return err
	}
	s.log = repo.NewCommitLog(s.repo)
	s.srvConfig = repo.NewServerConfig(s.repo)
	
	s.factory = message.NewConcreteMsgFactory()
	s.handler = server.NewServerAction(s.repo, s.log, s.srvConfig, s, s.factory, s)
	s.killch = make(chan bool, 1) // make it buffered to unblock sender
	s.status = protocol.ELECTING
	
	return nil
}

/////////////////////////////////////////////////////////////////////////////
// ServerCallback Interface
/////////////////////////////////////////////////////////////////////////////

func (s *fakeServer) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {
}

func (s *fakeServer) UpdateStateOnCommit(txnid common.Txnid, key string) {
}

func (s *fakeServer) GetStatus() protocol.PeerStatus {
	return s.status 
}

func (s *fakeServer) UpdateWinningEpoch(epoch uint32) {
}

/////////////////////////////////////////////////////////////////////////////
// QuorumVerifier 
/////////////////////////////////////////////////////////////////////////////

func (s *fakeServer) HasQuorum(count int) bool {
	ensembleSz := s.handler.GetEnsembleSize() - 1
	return count > int(ensembleSz / 2)
}
