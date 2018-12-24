package rabbit

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
	"tmp/DataDumper/config"
)

type Connector struct {
	uri   string
	con   *amqp.Connection
	close chan *amqp.Error
	ch    *amqp.Channel
	sync.Mutex
	waitAck int
}

func Make(conf config.RabbitConfig) (*Connector, error) {
	log.Printf("Connecting to Rabbit: %s:%v \n", conf.Host, conf.Port)
	url := fmt.Sprintf("amqp://%s:%s@%s:%v/", conf.User, conf.Pass, conf.Host, conf.Port)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("Connect to rabbit failed. " + err.Error())
	}

	ch, err := connection.Channel()
	if err != nil {
		return nil, errors.New("Create rabbit channel failed. " + err.Error())
	}

	rabbitConn := &Connector{
		url,
		connection,
		make(chan *amqp.Error),
		ch,
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
			connector.ch, err = connector.con.Channel()
			if err != nil {
				_ = connector.con.Close()
				log.Fatal("Create rabbit channel failed. ", err.Error())
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
	defer connector.Unlock()

	var c chan amqp.Confirmation
	if connector.waitAck > 0 {
		c = connector.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	err := connector.ch.Publish(
		"",    // exchange
		queue, // routing key
		true,  // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
	if err != nil {
		return err
	}

	if connector.waitAck > 0 {
		log.Println("Waiting for ack.")
		timer := time.After(time.Duration(connector.waitAck) * time.Second)
		select {
		case result := <-c:
			if result.Ack {
				return nil
			} else {
				return errors.New("error with delivery ")
			}
		case <-timer:
			return errors.New("error with delivery, ack timed out ")
		}
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
