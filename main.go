package main

import (
	"flag"
	"github.com/juju/fslock"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"
	"tmp/DataDumper/config"
	"tmp/DataDumper/rabbit"
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

	ticker := time.Tick(time.Second)
	for {
		select {
		case <-exit:
			log.Println("Buy-buy!")
		case <-ticker:
			for _, t := range conf.Tasks {
				task := t
				files, err := ioutil.ReadDir(t.InDir)
				if err != nil {
					log.Println("Error with read dir: " + err.Error())
					continue
				}
				for _, f := range files {
					if f.IsDir() {
						continue
					}

					path := t.InDir + string(os.PathSeparator) + f.Name()
					j := &Job{
						path,
						fslock.New(path),
						&task,
					}

					j.process()
				}
			}
		}
	}
}

func (j *Job) process() {
	log.Println("Sending file: " + j.Path)

	er := j.L.TryLock()
	if er != nil {
		log.Println("Error with locking file: " + j.Path)
		return
	}

	if err := j.L.Unlock(); err != nil { // looks like windows is not able to read from locked file :(
		log.Println("File unlock failed.", err.Error())
		return
	}

	b, err := ioutil.ReadFile(j.Path)
	if err != nil {
		log.Println("File read failed.", err.Error())
		moveFailed(j)
		return
	}

	err = rabbitConn.Publish(j.T.Queue, b)

	if err == nil {
		log.Println("File processed. " + j.Path)
		moveSuccess(j)
	} else {
		log.Println("Send to rabbit failed. ", err.Error())
		moveFailed(j)
	}
}

func moveSuccess(j *Job) {
	newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}

func moveFailed(j *Job) {
	newPath := j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}
