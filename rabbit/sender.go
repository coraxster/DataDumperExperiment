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
		log.Println("[ERROR] open channel failed: ", err)
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

	finalize(sentJobs)
}

func send(ch *amqp.Channel, jobs []*job.Job, closeCh chan *amqp.Error) []*job.Job {
	sentJobs := make([]*job.Job, 0, len(jobs))
	for _, j := range jobs {
		b, err := j.Prepare()
		if err != nil {
			log.Println("[WARNING] file read error: ", err)
			continue
		}
		if len(b) == 0 {
			j.Failed()
			continue
		}

		select {
		case <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			return sentJobs
		default:
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
			log.Println("[ERROR] send error: ", err)
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
}

func finalize(jobs []*job.Job) {
	result := make(map[job.Status]int)
	for _, j := range jobs {
		result[j.S]++
		if j.S == job.StatusPrepared {
			j.Failed()
		}
	}
	log.Printf("[DEBUG] Processed stat. New: %d, prepared: %d, success: %d, failed: %d, error: %d \n", result[job.StatusNew], result[job.StatusPrepared], result[job.StatusSuccess], result[job.StatusFailed], result[job.StatusError])
}
