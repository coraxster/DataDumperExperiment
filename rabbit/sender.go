package rabbit

import (
	"github.com/coraxster/DataDumper/job"
	"github.com/streadway/amqp"
	"log"
)

const MaxParallel = 10

type Sender struct {
	Connector *Connector
	WaitAck   bool
}

func (p *Sender) Send(jobs []*job.Job) {
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
				p.sendChunk(chunk)
			}
			doneCh <- true
		}()
	}

	for i := workersCount; i > 0; i-- {
		<-doneCh
	}
}

func (p *Sender) sendChunk(jobs []*job.Job) {
	defer func() {
		if r := recover(); r != nil { // just in case
			log.Println("[ERROR] sendChunk panics: ", r)
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

	var ackCh chan amqp.Confirmation
	if p.WaitAck {
		err := ch.Confirm(false)
		if err != nil {
			log.Println("[WARNING] rabbit apply ack mode failed: ", err)
			return
		}
		ackCh = ch.NotifyPublish(make(chan amqp.Confirmation, len(jobs)))
	}

	sentJobs := make([]*job.Job, 0, len(jobs))
	for _, j := range jobs {
		b, err := j.Bytes()
		if err != nil {
			log.Println("[WARNING] file read error: ", err)
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
			log.Println("[WARNING] send error: ", err)
			j.Failed()
		} else {
			if !p.WaitAck {
				j.Success()
			}
			sentJobs = append(sentJobs, j)
		}
	}

	if p.WaitAck {
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
			}
		}
	}
}
