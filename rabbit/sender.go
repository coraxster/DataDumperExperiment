package rabbit

import (
	"github.com/coraxster/DataDumper/job"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const MaxParallel = 10

type Sender struct {
	Connector *Connector
}

func (p *Sender) Process(jobs []*job.Job) {
	chunks := job.Split(jobs, 50)

	workersCount := MaxParallel
	if workersCount > len(chunks) {
		workersCount = len(chunks)
	}

	inCh := make(chan []*job.Job)
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
				p.processChunk(chunk)
			}
			doneCh <- true
		}()
	}

	for i := workersCount; i > 0; i-- {
		<-doneCh
	}
}

func (p *Sender) processChunk(jobs []*job.Job) {
	defer func() {
		if r := recover(); r != nil { // just in case
			log.Println("[ERROR] processChunk panics: ", r)
		}
	}()

	ch, err := p.Connector.Channel()
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

}

func send(ch *amqp.Channel, jobs []*job.Job, closeCh chan *amqp.Error) []*job.Job {
	sentJobs := make([]*job.Job, 0, len(jobs))
	for _, j := range jobs {
		select {
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			return sentJobs
		default:
		}

		b, err := j.Prepare()
		if err != nil {
			log.Println("[WARNING] file read error: ", err)
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
			log.Println("[WARNING] send error: ", err)
			j.Failed()
		} else {
			sentJobs = append(sentJobs, j)
		}
	}
	return sentJobs
}

func waitAcks(sentJobs []*job.Job, ackCh chan amqp.Confirmation, closeCh chan *amqp.Error) {
	timeOut := time.After(10 * time.Second)
	for range sentJobs {
		select {
		case result, ok := <-ackCh:
			if !ok { // looks like channel closed
				log.Println("[WARNING] channel closed.")
				return
			}
			if result.Ack {
				sentJobs[result.DeliveryTag-1].Success()
			} else {
				sentJobs[result.DeliveryTag-1].Failed()
			}
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			return
		case <-timeOut:
			log.Println("[WARNING] wait ack timed out")
			return
		}
	}

	for _, j := range sentJobs {
		if j.S == job.StatusPrepared { // did not receive ack
			j.Failed()
		}
	}
}
