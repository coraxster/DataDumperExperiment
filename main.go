package main

import (
	"flag"
	"github.com/coraxster/DataDumper/config"
	"github.com/coraxster/DataDumper/job"
	"github.com/coraxster/DataDumper/rabbit"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"
)

var version string
var conf *config.Config

func init() {
	log.Println("DataDumper2. Version: ", version)
	confFile := flag.String("config", "config.json", "config json file path")
	flag.Parse()
	var err error
	conf, err = config.Load(*confFile)
	if err != nil {
		log.Fatal("Config load failed. ", err)
	}
}

func main() {
	rabbitConn, err := rabbit.Make(conf.Rabbit)
	if err != nil {
		log.Fatal("Rabbit init failed. ", err)
	}

	if rabbitConn.SeedQueues(conf.GetQueues()) != nil {
		log.Fatal("Seed rabbit queues failed. ", err)
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	fini := make(chan bool)
	go func() {
		sendDirs(exit, rabbit.Sender{
			Connector: rabbitConn,
			WaitAck:   conf.Rabbit.WaitAck,
		})
		fini <- true
	}()

	//... add consumer-cleaner

	log.Println("Service started.")

	<-fini
	log.Println("See ya!")
}

func sendDirs(exit chan os.Signal, s rabbit.Sender) {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-exit:
			return
		case <-ticker:
			start := time.Now()
			var jobs []*job.Job
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
					jobs = append(jobs, &job.Job{
						Path: path,
						T:    &task,
					})
				}
			}
			if len(jobs) == 0 {
				continue
			}
			log.Printf("Got %v jobs.\n", len(jobs))
			s.Send(jobs)
			elapsed := time.Since(start)
			log.Printf("Dirs walk took %s\n", elapsed)
		}
	}
}
