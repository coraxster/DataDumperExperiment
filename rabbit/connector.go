package rabbit

import (
	"errors"
	"fmt"
	"github.com/coraxster/DataDumper/config"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Connector struct {
	uri   string
	conns map[*amqp.Connection]bool
	close chan *amqp.Error
	alive bool
	sync.RWMutex
}

func Make(conf config.RabbitConfig) (*Connector, error) {
	rabbitConn := &Connector{
		fmt.Sprintf("amqp://%s:%s@%s:%v/", conf.User, conf.Pass, conf.Host, conf.Port),
		make(map[*amqp.Connection]bool),
		make(chan *amqp.Error),
		false,
		sync.RWMutex{},
	}

	for i := 0; i < 10; i++ {
		err := rabbitConn.connect()
		if err != nil {
			return nil, err
		}
	}

	log.Println("[INFO] rabbit connected: ", len(rabbitConn.conns))

	go rabbitConn.support()

	return rabbitConn, nil
}

func (connector *Connector) IsAlive() bool {
	connector.RLock()
	defer connector.RUnlock()

	return len(connector.conns) > 0
}

func (connector *Connector) connect() error {
	conn, err := amqp.Dial(connector.uri)
	if err != nil {
		return errors.New("Connect to rabbit failed. " + err.Error())
	}
	connector.Lock()
	connector.conns[conn] = true
	connector.Unlock()

	lost := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err := <-lost
		log.Println("[ERROR] Connection failed. Error: ", err.Error())
		connector.Lock()
		delete(connector.conns, conn)
		connector.Unlock()
		connector.close <- err
	}()

	return nil
}

func (connector *Connector) support() {
	for {
		<-connector.close
		log.Println("[INFO] Try to reconnect.")
		for tries := 1; ; tries++ {
			power := time.Duration(tries)
			if tries > 30 {
				power = time.Duration(60)
			}
			err := connector.connect()
			if err != nil {
				log.Printf("[WARNING] Rabbit reconnect failed(%v). Error: %s \n", tries, err.Error())
				time.Sleep(500 * power * time.Millisecond)
				continue
			}
			log.Println("[INFO] Rabbit connected.")
			break
		}
	}
}

func (connector *Connector) Channel() (ch *amqp.Channel, err error) {
	var conn *amqp.Connection
	rand.Seed(int64(time.Now().Nanosecond()))
	connector.RLock()
	n := rand.Intn(len(connector.conns))
	for conn = range connector.conns {
		if n == 0 {
			break
		}
		n--
	}

	connector.RUnlock()

	ch, err = conn.Channel()
	if err != nil {
		return
	}
	err = ch.Confirm(false)
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

	err = ch.Close()
	if err != nil {
		return err
	}
	return nil
}
