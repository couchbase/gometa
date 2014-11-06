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
