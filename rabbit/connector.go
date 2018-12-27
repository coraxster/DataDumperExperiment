package rabbit

import (
	"errors"
	"fmt"
	"github.com/coraxster/DataDumper/config"
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
		log.Println("Connection failed. Error: ", lost.Error())
		connector.Lock()
		log.Println("Try to reconnect.")
		for tries := 1; ; tries++ {
			power := time.Duration(tries)
			if tries > 30 {
				power = time.Duration(60)
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

func (connector *Connector) Channel() (ch *amqp.Channel, err error) {
	defer func() {
		if r := recover(); r != nil {
			ch = nil
			err = errors.New(fmt.Sprintf("Channel() panics: %s", r))
		}
	}()

	connector.Lock()
	ch, err = connector.con.Channel()
	connector.Unlock()
	return
}

func (connector *Connector) SeedQueues(queues []string) error {
	ch, err := connector.Channel()
	if err != nil {
		return err
	}
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
			return errors.New("Declare rabbit queues failed. " + err.Error())
		}
	}
	return nil
}
