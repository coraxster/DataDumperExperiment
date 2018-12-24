package main

import (
	"fmt"
	"github.com/juju/fslock"
	"time"
)

func main() {

	l := fslock.New("/Users/dmitry.kuzmin/dev/go/src/tmp/DataDumper/data/1in/one")
	er := l.Lock()
	fmt.Println(er)

	time.Sleep(10 + time.Minute)
}
