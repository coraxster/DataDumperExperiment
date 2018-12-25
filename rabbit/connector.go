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
	log.Printf("Connecting to Rabbit: %s:%v \n", conf.Host, conf.Port)
	url := fmt.Sprintf("amqp://%s:%s@%s:%v/", conf.User, conf.Pass, conf.Host, conf.Port)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("Connect to rabbit failed. " + err.Error())
	}

	rabbitConn := &Connector{
		url,
		connection,
		make(chan *amqp.Error),
		sync.Mutex{},
		conf.WaitAck,
	}

	log.Println("Rabbit connected.")
	rabbitConn.con.NotifyClose(rabbitConn.close)

	go rabbitConn.support()

	return rabbitConn, nil
}

func (connector *Connector) support() {
	var err error
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
			connector.con, err = amqp.Dial(connector.uri)
			if err != nil {
				log.Printf("Rabbit(%v) reconnect failed. Error: %s", tries, err.Error())
				time.Sleep(500 * power * time.Millisecond)
				continue
			}
			log.Println("Rabbit connected.")
			tries = 0
			connector.close = make(chan *amqp.Error)
			connector.con.NotifyClose(connector.close)
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
