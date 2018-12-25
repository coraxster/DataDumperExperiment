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

			// тупо запараллелим процессинг, работает чуть быстрее, но убивает дисковый io
			// поэтому наверно не вариант
			// если параллелить, то несильно и именно открытие файла,
			// что бы отловить кейсы, когда os тупит и долго принимает решение о локе

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
					ch := rabbitConn.Channel()
					clCh := ch.NotifyClose(make(chan *amqp.Error))
					var ackCh chan amqp.Confirmation
					if conf.Rabbit.WaitAck > 0 {
						ackCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
					}
				forLoop:
					for {
						select {
						case j, ok := <-inCh:
							if !ok {
								break forLoop
							}
							log.Println("Sending file: " + j.Path)
							err := j.process(ch, ackCh)
							if err != nil {
								log.Println(err.Error())
								j.moveFailed()
							} else {
								j.moveSuccess()
							}
						case err = <-clCh:
							ch = rabbitConn.Channel()
						}
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

func (j *Job) process(ch *amqp.Channel, ackCh chan amqp.Confirmation) (err error) {
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
	if err != nil {
		return
	}

	if conf.Rabbit.WaitAck > 0 {
		log.Println("Waiting for ack.")
		timer := time.After(time.Duration(conf.Rabbit.WaitAck) * time.Second)
		select {
		case result := <-ackCh:
			if result.Ack {
				return
			} else {
				return errors.New("error with delivery, bad ACK ")
			}
		case <-timer:
			return errors.New("error with delivery, ack timed out ")
		}
	}
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
