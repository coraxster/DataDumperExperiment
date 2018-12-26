package main

import (
	"./config"
	"./rabbit"
	"errors"
	"flag"
	"github.com/streadway/amqp"
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
var conf *config.Config
var rabbitConn *rabbit.Connector

const MaxParallel = 10

func init() {
	log.Println("Version: ", version)
}

func main() {
	confFile := flag.String("config", "config.json", "config json file path")
	flag.Parse()
	var err error
	conf, err = config.Load(*confFile)
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
			log.Printf("Got %v jobs: ", len(jobs))
			multiProcess(jobs)
			elapsed := time.Since(start)
			log.Printf("Dirs walk took %s", elapsed)
		}
	}
}

// тупо запараллелим процессинг, работает чуть быстрее, но убивает дисковый io
// поэтому наверно не вариант
// если параллелить, то несильно и именно открытие файла,
// что бы отловить кейсы, когда os тупит и долго принимает решение о локе
//
func multiProcess(jobs []*Job) {
	chunks := split(jobs, 50)

	workersCount := MaxParallel
	if workersCount > len(chunks) {
		workersCount = len(chunks)
	}
	inCh := make(chan []*Job)
	doneCh := make(chan bool)
	go func() {
		for _, chunk := range chunks {
			inCh <- chunk
		}
		close(inCh)
	}()
	for i := workersCount; i > 0; i-- {
		go func() {
			for chunk := range inCh {
				processChunk(chunk)
			}
			doneCh <- true
		}()
	}
	for i := workersCount; i > 0; i-- {
		<-doneCh
	}
}

func split(jobs []*Job, lim int) [][]*Job {
	var chunk []*Job
	chunks := make([][]*Job, 0, len(jobs)/lim+1)
	for len(jobs) >= lim {
		chunk, jobs = jobs[:lim], jobs[lim:]
		chunks = append(chunks, chunk)
	}
	if len(jobs) > 0 {
		chunks = append(chunks, jobs[:])
	}
	return chunks
}

func processChunk(jobs []*Job) {
	ch := rabbitConn.Channel()
	defer func() {
		err := ch.Close()
		if err != nil {
			log.Printf("Channel close error %s", err.Error())
		}
	}()

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	var ackCh chan amqp.Confirmation
	if conf.Rabbit.WaitAck {
		ackCh = ch.NotifyPublish(make(chan amqp.Confirmation, len(jobs)))
	}

	sentJobs := make([]*Job, 0, len(jobs))
	for _, j := range jobs {
		err := j.process(ch)
		if err != nil {
			log.Println(err.Error())
			j.moveFailed()
		} else {
			sentJobs = append(sentJobs, j)
		}
	}

	if conf.Rabbit.WaitAck {
		for range sentJobs {
			select {
			case result, ok := <-ackCh:
				if !ok { // looks like channel closed
					log.Println("channel closed. ")
					return
				}
				if result.Ack {
					sentJobs[result.DeliveryTag-1].moveSuccess()
				} else {
					sentJobs[result.DeliveryTag-1].moveFailed()
				}
			case err := <-closeCh: // looks like channel closed
				log.Println("channel closed. " + err.Error())
				return
			}
		}
	}
}

func (j *Job) process(ch *amqp.Channel) (err error) {
	f, err := os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer func() {
		if fErr := f.Close(); fErr != nil {
			if err != nil {
				err = errors.New(err.Error() + fErr.Error())
			} else {
				err = fErr
			}
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		err = errors.New("File getting info failed. " + err.Error())
		return
	}

	if stat.Size() == 0 {
		return
	}

	b := make([]byte, stat.Size())
	_, err = f.Read(b)
	if err != nil {
		err = errors.New("File read failed. " + err.Error())
		return
	}
	err = ch.Publish(
		"",        // exchange
		j.T.Queue, // routing key
		true,      // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        b,
		})
	return
}

func (j *Job) moveSuccess() {
	newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}

func (j *Job) moveFailed() {
	newPath := j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}
