package common 

import (
	"log"
)

/////////////////////////////////////////////////////////////////////////////
// Utility 
/////////////////////////////////////////////////////////////////////////////

type FuncToRun func()()

func SafeRun(funcName string, f FuncToRun) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in %s() : %s\n", funcName, r)
		}
	}()
	
	f()	
}