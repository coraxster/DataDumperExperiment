package main

import (
	"./config"
	"./rabbit"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

type Job struct {
	Path string
	T    *config.Task
	F    *os.File
}

var version string
var rabbitConn *rabbit.Connector

func init() {
	log.Println("Version: ", version)
}

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
	jDone := make(chan *Job)
	finish := make(chan bool)

	go stage1(s1, conf, exit, jDone)
	go stage2(s2, s1, jDone)

	go func() {
		stage3(jDone, s2)
		finish <- true
	}()

	log.Println("Service started.")

	<-finish
}

func stage1(s1 chan<- *Job, conf *config.Config, exit chan os.Signal, jDone <-chan *Job) {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-exit:
			close(s1)
			return
		case <-ticker:
			inWorkCount := 0
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
					select {
					case s1 <- &Job{
						path,
						&task,
						nil,
					}:
						inWorkCount++
					case <-jDone:
						s1 <- &Job{
							path,
							&task,
							nil,
						}
					}
				}
			}
			for ; inWorkCount > 0; inWorkCount-- {
				<-jDone
			}
		}
	}
}

// sometimes we have problems with locking certain files.
// So lets lock them not blocking concurrently and push further successful
func stage2(s2 chan<- *Job, s1 <-chan *Job, jDone chan<- *Job) {
	s2Done := make(chan bool)
	for i := 10; i > 0; i-- {
		go func() {
			var err error
			for j := range s1 {
				j.F, err = os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
				if err != nil {
					log.Println("File open failed.", err.Error())
					jDone <- j
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

func stage3(jDone chan<- *Job, s2 <-chan *Job) {
	for j := range s2 {
		log.Println("Sending file: " + j.Path)

		stat, err := j.F.Stat()
		if err != nil {
			log.Println("File getting info failed.", err.Error())
			moveFailed(j)
			jDone <- j
			continue
		}

		if stat.Size() == 0 {
			log.Println("Got empty file.")
			moveSuccess(j)
			jDone <- j
			continue
		}

		b := make([]byte, stat.Size())
		_, err = j.F.Read(b)

		if err != nil {
			log.Println("File read failed.", err.Error())
			moveFailed(j)
			jDone <- j
			continue
		}

		if err = rabbitConn.Publish(j.T.Queue, b); err == nil {
			log.Println("File processed. " + j.Path)
			moveSuccess(j)
		} else {
			log.Println("Send to rabbit failed. ", err.Error())
			moveFailed(j)
		}
		jDone <- j
	}
}

func moveSuccess(j *Job) {
	err := j.F.Close()
	if err != nil {
		log.Println("File close failed.", err.Error())
		return
	}
	newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err = os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}

func moveFailed(j *Job) {
	err := j.F.Close()
	if err != nil {
		log.Println("File close failed.", err.Error())
		return
	}
	newPath := j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err = os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}
