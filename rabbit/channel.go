package rabbit

import "github.com/streadway/amqp"

type Channel interface {
	NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Confirm(noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Close() error
	NotifyClose(chan *amqp.Error) chan *amqp.Error
}
