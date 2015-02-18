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
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/server"
	"os"
)

// Temporary dependency until logging moves out. Avoids stub methods.
import "github.com/couchbase/indexing/secondary/logging"

func stdinWatcher() {
	b := make([]byte, 1)

	for {
		_, err := os.Stdin.Read(b)
		if err != nil {
			log.Current.Errorf("Got %s when reading stdin. Terminating.", err.Error())
			break
		}

		if b[0] == '\n' || b[0] == '\r' {
			log.Current.Errorf("Got new line on a stdin. Terminating.")
			break
		}
	}

	os.Exit(0)
}

//
// main function
//
func main() {
	var isClient bool
	var isWatcher bool
	var config string
	var watchStdin bool

	// Temporary dependency until logging moves out. Avoids stub methods.
	log.Current = &logging.SystemLogger

	flag.BoolVar(&isClient, "client", false, "run as test client")
	flag.BoolVar(&isWatcher, "watcher", false, "run as watcher")
	flag.StringVar(&config, "config", "", "path for configuration file")
	flag.BoolVar(&watchStdin, "watch-stdin", true,
		"watch standard input and terminate on EOL or EOF")
	flag.Parse()

	if isClient {
		runTestClient(config)
		os.Exit(0)
	}

	if isWatcher {
		runWatcher(config)
		os.Exit(0)
	}

	if watchStdin {
		go stdinWatcher()
	}

	err := server.RunServer(config)
	if err != nil {
		log.Current.Errorf("Encounter Error = %s. Terminate server", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
