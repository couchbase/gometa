package main

import (
	"flag"
	"github.com/couchbase/gometa/server"
	"log"
	"os"
)

//
// main function
//
func main() {

	var isClient string
	var isWatcher string
	var config string
	flag.StringVar(&isClient, "client", "false", "run as test client")
	flag.StringVar(&isWatcher, "watcher", "false", "run as watcher")
	flag.StringVar(&config, "config", "", "path for configuration file")
	flag.Parse()

	if isClient == "true" {
		runTestClient(config)
		os.Exit(0)
	}

	if isWatcher == "true" {
		runWatcher(config)
		os.Exit(0)
	}

	err := server.RunServer(config)
	if err != nil {
		log.Printf("Encounter Error = %s. Terminate server", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
