package rabbit

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Connector interface {
	IsAlive() bool
	Channel() (Channel, error)
	SeedQueues([]string) error
	io.Closer
}

type connector struct {
	uri   string
	conns map[*amqp.Connection]bool
	close chan *amqp.Error
	alive bool
	sync.RWMutex
	closed chan struct{}
}

func Make(user, pass, host, port string, connNumber int) (Connector, error) {
	rabbitConn := &connector{
		fmt.Sprintf("amqp://%s:%s@%s:%v/", user, pass, host, port),
		make(map[*amqp.Connection]bool),
		make(chan *amqp.Error),
		false,
		sync.RWMutex{},
		make(chan struct{}),
	}

	for i := 0; i < connNumber; i++ {
		err := rabbitConn.connect()
		if err != nil {
			return nil, err
		}
	}

	log.Println("[INFO] rabbit connected:", len(rabbitConn.conns))

	go rabbitConn.support()

	return rabbitConn, nil
}

func (connector *connector) IsAlive() bool {
	select {
	case <-connector.closed:
		return false
	default:
	}
	connector.RLock()
	defer connector.RUnlock()
	return len(connector.conns) > 0
}

func (connector *connector) connect() error {
	conn, err := amqp.Dial(connector.uri)
	if err != nil {
		return errors.Wrap(err, "connect to rabbit failed")
	}
	connector.Lock()
	connector.conns[conn] = true
	connector.Unlock()

	lost := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err := <-lost
		select {
		case <-connector.closed:
			return
		default:
		}
		log.Println("[ERROR] Connection failed. Error: ", err)
		connector.Lock()
		delete(connector.conns, conn)
		connector.Unlock()
		connector.close <- err
	}()

	return nil
}

func (connector *connector) support() {
	for {
		<-connector.close
		for tries := 1; ; tries++ {
			select {
			case <-connector.closed:
				return
			default:
			}
			log.Println("[INFO] Try to reconnect.")
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

func (connector *connector) Channel() (ch Channel, err error) {
	connector.RLock()
	if !connector.IsAlive() {
		return nil, errors.New("connector is not alive")
	}
	var conn *amqp.Connection
	rand.Seed(int64(time.Now().Nanosecond()))
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

func (connector *connector) SeedQueues(queues []string) error {
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
			return errors.Wrap(err, "declare rabbit queues failed")
		}
	}

	err = ch.Close()
	if err != nil {
		return err
	}
	return nil
}

func (connector *connector) Close() error {
	connector.RLock()
	defer connector.RUnlock()
	close(connector.closed)
	var errs error = nil
	for c := range connector.conns {
		err := c.Close()
		if err != nil {
			log.Println("[WARNING] error while closing:", err.Error())
			errs = errors.Wrap(errs, err.Error())
		}
	}
	return errs
}
