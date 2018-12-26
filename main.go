package main

import (
	"flag"
	"github.com/coraxster/DataDumper/config"
	"github.com/coraxster/DataDumper/rabbit"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"
)

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
			process(jobs)
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
func process(jobs []*Job) {
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

func processChunk(jobs []*Job) {
	ch := rabbitConn.Channel()
	if conf.Rabbit.WaitAck {
		err := ch.Confirm(false)
		if err != nil {
			log.Println("rabbit apply ack mode failed:", err)
			return
		}
	}
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
		b, err := j.Bytes()
		if err != nil {
			log.Println("File read error: ", err)
			j.Failed()
			continue
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
		if err != nil {
			log.Println("Send error:", err)
			j.Failed()
		} else {
			if !conf.Rabbit.WaitAck {
				j.Success()
			}
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
					sentJobs[result.DeliveryTag-1].Success()
				} else {
					sentJobs[result.DeliveryTag-1].Failed()
				}
			case err := <-closeCh: // looks like channel closed
				log.Println("channel closed. " + err.Error())
				return
			}
		}
	}
}
