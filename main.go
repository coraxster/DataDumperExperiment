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

	ticker := time.Tick(time.Second)
fl:
	for {
		select {
		case <-exit:
			log.Println("See ya!")
			break fl
		case <-ticker:
			start := time.Now()

			var jobs []*Job
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
					jobs = append(jobs, &Job{
						path,
						&task,
					})
				}
			}

			if len(jobs) == 0 {
				continue
			}

			inCh := make(chan *Job)
			doneCh := make(chan bool)

			go func() {
				for _, j := range jobs {
					inCh <- j
				}
				close(inCh)
			}()

			for i := 10; i > 0; i-- {
				go func() {
					for j := range inCh {
						j.process()
					}
					doneCh <- true
				}()
			}

			for i := 10; i > 0; i-- {
				<-doneCh
			}
			elapsed := time.Since(start)
			log.Printf("Dirs walk took %s", elapsed)
		}
	}
}

func (j *Job) process() {
	log.Println("Sending file: " + j.Path)

	f, err := os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
	if err != nil {
		log.Println("File open failed.", err.Error())
		return
	}

	stat, err := f.Stat()
	if err != nil {
		log.Println("File getting info failed.", err.Error())
		err = f.Close()
		if err != nil {
			log.Println("File close failed.", err.Error())
			return
		}
		moveFailed(j)
		return
	}

	b := make([]byte, stat.Size())
	_, err = f.Read(b)

	err = rabbitConn.Publish(j.T.Queue, b)

	err = f.Close()
	if err != nil {
		log.Println("File close failed.", err.Error())
		return
	}
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
