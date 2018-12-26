package rabbit

import (
	"../config"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type Connector struct {
	uri   string
	con   *amqp.Connection
	close chan *amqp.Error
	ch    *amqp.Channel
	sync.Mutex
	waitAck bool
	ConfCh  chan amqp.Confirmation
}

func Make(conf config.RabbitConfig) (*Connector, error) {
	rabbitConn := &Connector{
		fmt.Sprintf("amqp://%s:%s@%s:%v/", conf.User, conf.Pass, conf.Host, conf.Port),
		nil,
		nil,
		nil,
		sync.Mutex{},
		conf.WaitAck,
		nil,
	}

	err := rabbitConn.connect()
	if err != nil {
		return nil, err
	}
	log.Println("Rabbit connected.")

	go rabbitConn.support()

	return rabbitConn, nil
}

func (connector *Connector) connect() error {
	var err error
	connector.con, err = amqp.Dial(connector.uri)
	if err != nil {
		return errors.New("Connect to rabbit failed. " + err.Error())
	}

	connector.ch, err = connector.con.Channel()
	if err != nil {
		return errors.New("Create rabbit channel failed. " + err.Error())
	}

	if connector.waitAck {
		if err = connector.ch.Confirm(false); err != nil {
			return errors.New("Set ACK confirmation mode failed. " + err.Error())
		}
		connector.ConfCh = connector.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}
	connector.close = connector.con.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (connector *Connector) support() {
	for {
		lost := <-connector.close
		log.Println("Connection failed. Error: ", lost)
		connector.Lock()
		log.Println("Try to reconnect.")
		for tries := 1; ; tries++ {
			power := time.Duration(1)
			if tries < 20 {
				power = time.Duration(tries)
			}
			err := connector.connect()
			if err != nil {
				log.Printf("Rabbit(%v) reconnect failed. Error: %s \n", tries, err.Error())
				time.Sleep(500 * power * time.Millisecond)
				continue
			}
			log.Println("Rabbit reconnected.")
			connector.close = connector.con.NotifyClose(make(chan *amqp.Error))
			connector.Unlock()
			break
		}
	}
}

func (connector *Connector) SeedQueues(queues []string) error {
	connector.Lock()
	defer connector.Unlock()

	for _, q := range queues {
		_, err := connector.ch.QueueDeclare(
			q, // name
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return errors.New("Open rabbit channel failed. " + err.Error())
		}
	}
	return nil
}

func (connector *Connector) Publish(queue string, data []byte) error {
	connector.Lock()
	err := connector.ch.Publish(
		"",    // exchange
		queue, // routing key
		true,  // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
	connector.Unlock()
	if err != nil {
		return err
	}

	if connector.waitAck {
		log.Println("Waiting for ack.")
		result, ok := <-connector.ConfCh
		if ok && result.Ack {
			return nil
		}
		return errors.New("error with delivery ")
	}
	return nil
}

func (connector *Connector) Consume(ch chan amqp.Delivery, queue string) error {
	channel, err := connector.con.Channel()
	if err != nil {
		return err
	}
	msgs, err := channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			ch <- d
		}
	}()
	return nil
}
