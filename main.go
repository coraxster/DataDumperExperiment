package main

import (
	"./config"
	"./rabbit"
	"flag"
	"github.com/juju/fslock"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

type Job struct {
	Path string
	L    *fslock.Lock
	T    *config.Task
}

var rabbitConn *rabbit.Connector

func main() {
	confFile := flag.String("config", "config.json", "config json file path")
	flag.Parse()

	conf, err := config.Load(*confFile)
	if err != nil {
		log.Fatal("Config load failed.", err.Error())
	}

	rabbitConn, err = rabbit.Make(conf.Rabbit)
	if err != nil {
		log.Fatal("Rabbit init failed.", err.Error())
	}

	if rabbitConn.SeedQueues(conf.GetQueues()) != nil {
		log.Fatal("Seed rabbit queues failed.", err.Error())
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	s1 := make(chan *Job, 10)
	s2 := make(chan *Job)
	sDone := make(chan *Job)
	finish := make(chan bool)

	go stage1(s1, conf, exit, sDone)
	go stage2(s2, s1, sDone)

	go func() {
		stage3(sDone, s2)
		finish <- true
	}()

	log.Println("Service started.")

	<-finish
}

func stage1(s1 chan<- *Job, conf *config.Config, exit chan os.Signal, sDone <-chan *Job) {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-exit:
			close(s1)
			return
		case <-ticker:
			for _, t := range conf.Tasks {
				task := t
				files, err := ioutil.ReadDir(t.InDir)
				if err != nil {
					log.Println("Error with read dir: " + err.Error())
				}
				for _, f := range files {
					path := t.InDir + string(os.PathSeparator) + f.Name()
					if !f.IsDir() {
						s1 <- &Job{
							path,
							fslock.New(path),
							&task,
						}
						<-sDone
					}
				}
			}
		}
	}
}

// sometimes we have problems with locking certain files.
// So lets lock them not blocking concurrently and push further successful
func stage2(s2 chan<- *Job, s1 <-chan *Job, sDone chan<- *Job) {
	s2Done := make(chan bool)
	for i := 10; i > 0; i-- {
		go func() {
			for j := range s1 {
				er := j.L.LockWithTimeout(5 * time.Second)
				if er != nil {
					log.Println("Error with locking file: " + j.Path)
					sDone <- j
					continue
				}
				s2 <- j
			}
			s2Done <- true
		}()
	}
	go func() {
		for i := 10; i > 0; i-- {
			<-s2Done
		}
		close(s2)
		close(s2Done)
	}()
}

func stage3(sDone chan<- *Job, s2 <-chan *Job) {
	for j := range s2 {
		log.Println("Sending file: " + j.Path)
		if err := j.L.Unlock(); err != nil { // looks like windows is not able to read from locked file :(
			log.Println("File unlock failed.", err.Error())
			sDone <- j
			continue
		}
		b, err := ioutil.ReadFile(j.Path)
		if err != nil {
			log.Println("File read failed.", err.Error())
			sDone <- j
			continue
		}

		newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
		err = rabbitConn.Publish(j.T.Queue, b)
		if err != nil {
			log.Println("Send to rabbit failed. ", err.Error())
			newPath = j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
		}

		err = os.Rename(j.Path, newPath)
		if err != nil {
			log.Println("File move failed. ", err.Error())
		}

		log.Println("File processed. " + j.Path)
		sDone <- j
	}
}
