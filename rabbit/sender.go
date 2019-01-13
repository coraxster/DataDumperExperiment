package rabbit

import (
	"github.com/coraxster/DataDumper/job"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const MaxParallel = 10

type Sender struct {
	Connector
}

func (s *Sender) Process(jobs []job.Job) {
	chunks := job.Split(jobs, 50)

	workersCount := MaxParallel
	if workersCount > len(chunks) {
		workersCount = len(chunks)
	}

	inCh := make(chan []job.Job)
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
				s.processChunk(chunk)
			}
			doneCh <- true
		}()
	}

	for i := workersCount; i > 0; i-- {
		<-doneCh
	}
}

func (s *Sender) processChunk(jobs []job.Job) {
	defer func() {
		if r := recover(); r != nil { // just in case
			log.Println("[ERROR] processChunk panics: ", r)
		}
	}()

	ch, err := s.Connector.Channel()
	if err != nil {
		log.Println("[WARNING] open channel failed: ", err)
		return
	}
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	defer func() {
		select {
		case <-closeCh:
			return
		default:
			err := ch.Close()
			if err != nil {
				log.Println("[WARNING] channel close error: ", err)
			}
		}
	}()

	ackCh := ch.NotifyPublish(make(chan amqp.Confirmation, len(jobs)))

	sentJobs := send(ch, jobs, closeCh)

	waitAcks(sentJobs, ackCh, closeCh)

	finish(jobs)
}

func send(ch *amqp.Channel, jobs []job.Job, closeCh chan *amqp.Error) []job.Job {
	sentJobs := make([]job.Job, 0, len(jobs))
	for _, j := range jobs {
		select {
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			return sentJobs
		default:
		}

		err := j.Prepare() // lock file
		if err != nil {
			log.Println("[WARNING] file lock error: ", err)
			continue
		}

		b, err := j.Bytes()
		if err != nil {
			log.Println("[WARNING] file read error: ", err)
			continue
		}

		if len(b) == 0 {
			j.SetStatus(job.StatusFailed)
			log.Println("[WARNING] got empty file: ", j.GetPath())
			continue
		}

		err = ch.Publish(
			"",           // exchange
			j.GetQueue(), // routing key
			true,         // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        b,
			})
		if err != nil {
			log.Println("[WARNING] send error: ", err)
			j.SetStatus(job.StatusFailed)
		} else {
			sentJobs = append(sentJobs, j)
		}
	}
	return sentJobs
}

func waitAcks(sentJobs []job.Job, ackCh chan amqp.Confirmation, closeCh chan *amqp.Error) {
	timeOut := time.After(10 * time.Second)
	for range sentJobs {
		select {
		case result, ok := <-ackCh:
			if !ok { // looks like channel closed
				log.Println("[WARNING] channel closed.")
				break
			}
			if result.Ack {
				sentJobs[result.DeliveryTag-1].SetStatus(job.StatusSuccess)
			} else {
				sentJobs[result.DeliveryTag-1].SetStatus(job.StatusFailed)
			}
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			break
		case <-timeOut:
			log.Println("[WARNING] wait ack timed out")
			break
		}
	}

	for _, j := range sentJobs {
		if j.GetStatus() == job.StatusLocked { // has not received ack
			j.SetStatus(job.StatusFailed)
		}
	}
}

func finish(jobs []job.Job) {
	for _, j := range jobs {
		if err := j.Finish(); err != nil {
			log.Println("[WARNING] job finishing error: ", err)
		}
	}
}
