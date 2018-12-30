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

var version = "unknown"
var conf *config.Config

func init() {
	log.Println("[INFO] DataDumper2. Version: ", version)
	confFile := flag.String("config", "config.json", "config json file path")
	flag.Parse()
	var err error
	conf, err = config.Load(*confFile)
	exitOnError(err, "Config load failed.")
}

func main() {
	rabbitConn, err := rabbit.Make(conf.Rabbit)
	exitOnError(err, "Rabbit init failed.")

	err = rabbitConn.SeedQueues(conf.GetQueues())
	exitOnError(err, "Seed rabbit queues failed.")

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	fini := make(chan bool)
	go func() {
		sendDirs(exit, rabbit.Sender{
			Connector: rabbitConn,
		})
		fini <- true
	}()

	//... add consumer-cleaner

	log.Println("[INFO] Service started.")

	<-fini
	log.Println("[INFO] See ya!")
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
					log.Println("[ERROR] Error with read dir: " + err.Error())
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
			log.Printf("[INFO] Got %v jobs.\n", len(jobs))
			s.Process(jobs)
			elapsed := time.Since(start)
			log.Printf("[INFO] Dirs walk took %s\n", elapsed)
		}
	}
}

func exitOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("[ERROR] %s: %s", msg, err)
	}
}
