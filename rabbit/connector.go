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
	sync.Mutex
	waitAck bool
}

func Make(conf config.RabbitConfig) (*Connector, error) {
	rabbitConn := &Connector{
		fmt.Sprintf("amqp://%s:%s@%s:%v/", conf.User, conf.Pass, conf.Host, conf.Port),
		nil,
		nil,
		sync.Mutex{},
		conf.WaitAck,
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
	connector.close = connector.con.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (connector *Connector) support() {
	for {
		lost := <-connector.close
		connector.Lock()
		log.Println("Connection failed. Error: ", lost.Error())
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
			log.Println("Rabbit connected.")
			connector.Unlock()
			break
		}
	}
}

func (connector *Connector) Channel() *amqp.Channel {
	connector.Lock()
	defer connector.Unlock()

	for {
		ch, err := connector.con.Channel()
		if err != nil {
			log.Println("rabbit channel create failed:", err.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		if connector.waitAck {
			err = ch.Confirm(false)
			if err != nil {
				log.Println("rabbit apply ack mode failed:", err.Error())
				time.Sleep(5 * time.Second)
				continue
			}
		}
		return ch
	}
}

func (connector *Connector) SeedQueues(queues []string) error {
	ch := connector.Channel()
	for _, q := range queues {
		_, err := ch.QueueDeclare(
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

func (connector *Connector) Consume(ch chan amqp.Delivery, queue string) error {
	channel := connector.Channel()

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
