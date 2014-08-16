package main

import (
	"os"
	"log"
	"flag"
	"message"
	"fmt"
	"net/rpc"
	"github.com/prataprc/collatejson"
	"common"
	"server"
	"bytes"
)

//
// main function
//
func main() {

	var isClient string
	flag.StringVar(&isClient, "client", "false", "run as test client")
	flag.Parse()
	
	if isClient == "true" {
		RunTestClient()
		os.Exit(0)
	}

	err := server.RunServer()
	if err != nil {
		log.Printf("Encounter Error = %s. Terminate server", err.Error())	
		os.Exit(1)
	}

	os.Exit(0) 
}

func RunTestClient() {

	// connect to the server
	var host string
	fmt.Printf("Enter server host\n") 
	n, err := fmt.Scanf("%s", &host) 
	if err != nil {
		fmt.Printf("Error : %s", err.Error())
		return
	}
	if n != 1 {
		fmt.Printf("Missing arugment")
	}
	client, err := rpc.DialHTTP("tcp", host) 
	if err != nil {
		fmt.Printf("Fail to create connection to server %s.  Error %s", host, err.Error())
		return
	}	
	
	// create a message factory 
	factory := message.NewConcreteMsgFactory() 
	
	for {
		// read command from console 
		var command, key, value string
		var repeat int
		fmt.Printf("Enter command(Add, Set, Delete)\n")
		_, err := fmt.Scanf("%s", &command)
		if err != nil {
			fmt.Printf("Error : %s", err.Error())
			continue
		}
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

		for i:=0; i < repeat; i++ {
			sendKey := fmt.Sprintf("%s-%d", key, i)
			sendValue := fmt.Sprintf("%s-%d", value, i)
			
			// convert command string to byte
			content, err := collateString(sendValue)
			if err != nil {
	    		log.Printf("ClientTest() : Fail to convert content into bytes. Error %s. ", err.Error()) 
			}		
	
			// create a request object and serialize it	
			request := factory.CreateRequest(uint64(1), uint32(common.GetOpCode(command)), sendKey, content)
			msg, err := common.Marshall(request)
			if err != nil {
	    		log.Printf("ClientTest() : Fail to marshall request message. Error %s. ", err.Error()) 
			}		
	
			// send serialized request object to server	
			var reply []byte 
			err = client.Call("RequestReceiver.NewRequest", msg, &reply)
			if err != nil {
	    		log.Printf("ClientTest() : Fail to call server %s. ", err.Error()) 
	    	}
		}
	}	
}

func collateString(key string) ([]byte, error) {
	if key == "" {
		return nil, nil
	}

	jsoncodec := collatejson.NewCodec()
	buf := new(bytes.Buffer)
	_, err := buf.Write(jsoncodec.EncodeString(key))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}	