
I) INTRODUCTION
---------------

Gometa is a GO implementation of a distributed metadata store. Gometa uses zookeeper Zab protocol for leader election and quorum write.  

Zookeeper is battle tested consensus protocol, being used in big deployment such as linked-in and Netflix.  Papers regarding zookeeper:

http://www.tcs.hut.fi/Studies/T-79.5001/reports/2012-deSouzaMedeiros.pdf

https://www.usenix.org/legacy/event/usenix10/tech/full_papers/Hunt.pdf

http://web.stanford.edu/class/cs347/reading/zab.pdf

II) BUILD
---------

Build the main.go

	cd $GOPATH/src
	mkdir -r github.com/couchbase
	cd github.com/couchbase
	git clone git@github.com:couchbase/gometa.git
	go build -o $GOPATH/bin/gometa gometa/server/main/main.go

You can then run bin/main as either a client or server. 

A) Run As Server
----------------

To run as server, you will need a ensemble of processes.   Each process requires 3 ports:

1) a UDP port for leader election

2) a TCP port to send messages with the leader of the ensemble

3) a TCP port to receive request from client

For each process, you can specify the ports of the ensemble as a configuration file.  The configuration file has 2 main sections:

1) Host - This section specify the 3 ports used by the server process.

2) Peer - This section specify the UDP (election) port and the TCP (message) port for the other processes in the ensemble.  

{

    "Host" : {

	       	"ElectionAddr" : "localhost:5001",

	        "MessageAddr"  : "localhost:5002",

	        "RequestAddr"  : "localhost:5003"

		    },

    "Peer" : [

	        {"ElectionAddr" : "localhost:6001",

	         "MessageAddr"  : "localhost:6002"},

            {"ElectionAddr" : "localhost:7001",

		     "MessageAddr"  : "localhost:7002}

     ]

}

You can then start the process as follow

	$GOPATh/bin/gometa -config="<config file path>"

Once you start this process, it will run leader election and try to connect to the other processes (localhost:6001 and localhost:7001).  The leader
election will select the leader from this ensemble.   The other 2 processes will act as followers.

You would want to run each process in a different directory, since each process will create a database file (of the same name).  So you want the
database file to be in different directory.


B) Run As Client
----------------

To run the manual test client, you will also need to specify the configuration file which specify the request port of the processes in the ensemble:

{

    "Peer" : [

	       	{"RequestAddr" : "localhost:5003"},

		    {"RequestAddr" : "localhost:6003"},

		    {"RequestAddr" : "localhost:7003"}

    ]

}

You can start the test client as follows:

main -client=true -config="config.json"

Once the client has started, it will ask you which process to connect you.  You can choose any of the process in the ensemble to process client request.

The client support 4 commands (Add, Set, Delete, Get).   For Add and Set, you can also specify the iteration count and the client will send out a series of calls to the server iteratively.

III) DEPENDENCY 
---------------

The metadata store uses forestdb.  You will need to get forestdb as well as goforestdb (forestdb GO wrapper).  It also uses protobuf for messaging.

go get github.com/couchbase/goforestdb

go get -u code.google.com/p/goprotobuf/{proto,protoc-gen-go}

IV) KEY BACKLOG
----------------

1) Support CAS

2) Suport learner

3) Support ns-server

4) Dynamic Configuration (add or remove node)

5) Compaction on commit log

6) Rolling upgrade

