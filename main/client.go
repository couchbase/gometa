package main

import (
	"os"
	"log"
	"fmt"
	"net/rpc"
	"github.com/couchbase/gometa/server"
	"bytes"
	json "encoding/json"
)

type Request struct {
	OpCode	string
	Key		string
	Value	[]byte
}

type Reply struct {
	Result	[]byte
}

func runTestClient(path string) {

	// connect to the server
	var host string
	
	if path == "" {
		fmt.Printf("Enter server host\n") 
		n, err := fmt.Scanf("%s", &host) 
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			return
		}
		if n != 1 {
			fmt.Printf("Missing arugment")
		}
	} else {
		file, err := os.Open(path) 
		if err != nil {
			return 
		}
	
		buffer := new(bytes.Buffer)
		_, err = buffer.ReadFrom(file)
		if err != nil {
			return 
		}

		var config server.Config	
		err = json.Unmarshal(buffer.Bytes(), &config) 
		if err != nil {
			return 
		}
		
		for i, peer := range config.Peer {
			fmt.Printf("\t%d - %s\n", i, peer.RequestAddr) 
		}
		var idx int
		fmt.Printf("Select Host (number)\n") 
		n, err := fmt.Scanf("%d", &idx) 
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			return
		}
		if n != 1 || idx >= len(config.Peer) {
			fmt.Printf("Invalid arugment")
		}
		
		host = config.Peer[idx].RequestAddr		
	}
	
	client, err := rpc.DialHTTP("tcp", host) 
	if err != nil {
		fmt.Printf("Fail to create connection to server %s.  Error %s", host, err.Error())
		return
	}	
	
	for {
		// read command from console 
		var command, key, value string
		var repeat int
		fmt.Printf("Enter command(Add, Set, Delete, Get)\n")
		_, err := fmt.Scanf("%s", &command)
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			continue
		}
		
		if command == "Add" || command == "Set" {
			fmt.Printf("Enter Starting Key\n")
			_, err = fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Printf("Error : %s", err.Error())
				continue
			}
			fmt.Printf("Enter Starting Value\n")
			_, err = fmt.Scanf("%s", &value)
			if err != nil {
				fmt.Printf("Error : %s", err.Error())
				continue
			}
			fmt.Printf("Enter Reptition\n")
			_, err = fmt.Scanf("%d", &repeat)
			if err != nil {
				fmt.Printf("Error : %s", err.Error())
				continue
			}
		} else if command == "Delete" || command == "Get" {
			fmt.Printf("Enter Key\n")
			_, err = fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Printf("Error : %s", err.Error())
				continue
			}
			value = "" 
			repeat = 1
		} else {
			fmt.Printf("Error : Unknown commond %s", command) 
		}

		for i:=0; i < repeat; i++ {
			var sendKey, sendValue string
			var content []byte
			
			if repeat > 1 {
				sendKey = fmt.Sprintf("%s-%d", key, i)
				sendValue = fmt.Sprintf("%s-%d", value, i)
			} else {
				sendKey = key
				sendValue = value 
			}
			
			// convert command string to byte
			if sendValue != "" {
				content = ([]byte)(sendValue)
			} else {
				content = nil
			}

	    	request := &Request{OpCode : command, Key : sendKey, Value : content}
	    	var reply *Reply
			err = client.Call("RequestReceiver.NewRequest", request, &reply)
			if err != nil {
	    		log.Printf("ClientTest() : Error from server : %s. ", err.Error()) 
	    	}
	    	
	    	if reply != nil  && reply.Result != nil {
				fmt.Printf("Result = %s, len(result) = %d\n", string(reply.Result), len(reply.Result))
	    	}
		}
	}	
}
