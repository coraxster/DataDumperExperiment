package rabbit

import (
	"context"
	"github.com/coraxster/DataDumper/job"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

const MaxParallel = 10

type Sender struct {
	Connector
}

func (s *Sender) Process(ctx context.Context, jobs []job.Job) {
	chunks := job.Split(jobs, 50)

	workersCount := MaxParallel
	if workersCount > len(chunks) {
		workersCount = len(chunks)
	}

	inCh := make(chan []job.Job)
	go func() {
		defer close(inCh)
		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				return
			case inCh <- chunk:
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(workersCount)
	for i := workersCount; i > 0; i-- {
		go func() {
			defer wg.Done()
			for chunk := range inCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				s.processChunk(chunk)
			}
		}()
	}
	wg.Wait()
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
}

func send(ch Channel, jobs []job.Job, closeCh chan *amqp.Error) []job.Job {
	sentJobs := make([]job.Job, 0, len(jobs))
	for _, j := range jobs {
		select {
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			return sentJobs
		default:
		}

		b, err := j.Bytes()
		if err != nil {
			log.Println("[WARNING] file read error: ", err)
			continue
		}

		if len(b) == 0 {
			log.Println("[WARNING] got empty file: ", j.GetPath())
			err = j.Finish(false)
			if err != nil {
				log.Println("[WARNING] job finishing error: ", err)
			}
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
			if err = j.Finish(false); err != nil {
				log.Println("[WARNING] job finishing error: ", err)
			}
		} else {
			sentJobs = append(sentJobs, j)
		}
	}
	return sentJobs
}

func waitAcks(sentJobs []job.Job, ackCh chan amqp.Confirmation, closeCh chan *amqp.Error) {
	timeOut := time.After(10 * time.Second)
	received := make([]bool, len(sentJobs))
fl:
	for range sentJobs {
		select {
		case result, ok := <-ackCh:
			if !ok { // looks like channel closed
				log.Println("[WARNING] channel closed.")
				break fl
			}
			received[result.DeliveryTag-1] = true
			err := sentJobs[result.DeliveryTag-1].Finish(result.Ack)
			if err != nil {
				log.Println("[WARNING] job finishing error: ", err)
			}
		case err := <-closeCh: // looks like channel closed
			log.Println("[WARNING] channel closed: ", err)
			break fl
		case <-timeOut:
			log.Println("[WARNING] wait ack timed out")
			break fl
		}
	}

	for i, j := range sentJobs {
		if !received[i] {
			err := j.Finish(false)
			if err != nil {
				log.Println("[WARNING] job finishing error: ", err)
			}
		}
	}
}
