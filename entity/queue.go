package entity 

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type OverflowMessagePoliticType uint

const (
	// Used by deafult, delete oldest msg
	DropOldestMsgOverflowMessagePoliticType OverflowMessagePoliticType = iota
	// Discard new msg
	DiscardNewMsgOverflowMessagePoliticType
)

type RabbitMQQueueConfig struct {
	// Queue name (use to bind with exchanges), if empty rabbit creates name.
	Name string
	// Has to rabbbitmq save queues on filesystem
	IsDurable bool
	// Delete queue if no one consumers from it
	DeleteWhenUnused bool
	// We have only one !!connection(not consumer, we can have several consumers and channels in connection)!! which can read from queue
	IsExclusive bool
	// No wait queue creation response, if something went wrong, it would raise exception
	IsNoWait bool
	// Additional arguments for queue
	Args map[string]any
}

// Set a message time to live in a queue.
func (c *RabbitMQQueueConfig) SetMessageTTL(ttl time.Duration) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-message-ttl"] = ttl.Milliseconds()
}

// Set timeout when a queue will be deleted if no one uses it.
// We don't have guarantees when a queue will be delete, we only know that it will be alive at least this time span.
func (c *RabbitMQQueueConfig) SetQueueExpireTime(t time.Duration) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-expires"] = t.Milliseconds()
}

// Set max message count in a queue.
func (c *RabbitMQQueueConfig) SetMaxMessageCount(messageCount uint64) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-max-length"] = messageCount 
}

// Set max messages size (bytes) in a queue.
func (c *RabbitMQQueueConfig) SetMaxMessageSizeBytes(byteCount uint64) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-max-lenght-bytes"] = byteCount 
}

// Set drop message politic.
func (c *RabbitMQQueueConfig) SetDropMsgPolitic(dropPolitic OverflowMessagePoliticType) error {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	switch dropPolitic {
	case DropOldestMsgOverflowMessagePoliticType: c.Args["x-overflow"] = "drop-head"
	case DiscardNewMsgOverflowMessagePoliticType: c.Args["x-overflow"] = "reject-publish"
	default:
		return fmt.Errorf("Unknown drop message policy %d use default", dropPolitic)
	}
	return nil
}

// Set a exchange, where redirect dead(necked or ttl) messages.
func (c *RabbitMQQueueConfig) SetDeadLetterExchange(deadLettersExchange string) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-dead-letter-exchange"] = deadLettersExchange 
}
// Set only one active consumer (other will wait until first consumer will died)
func (c *RabbitMQQueueConfig) SetSingleActiveConusmer() {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-single-active-consumer"] = true 
}

// Set dead letter key, what will be used to link dead letter exchange with next queue (if it isn't set previous key will be used).
func (c *RabbitMQQueueConfig) SetDeadLetterKey(deadLetterKey string) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-dead-letter-routing-key"] = deadLetterKey 
}

// Set max message priority. If message has priority more it will have the maxPriority in queue, if message doesn't have priority it will have priority = 0. Optimal priority values 0-5.
func (c *RabbitMQQueueConfig) SetMaxPriority(maxPriority uint8) {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-max-priority"] = maxPriority 
}

// Preserve messages on disk. It is very helpful if we don't want use a lot of RAM, but it slows down performance.
func (c *RabbitMQQueueConfig) SetLazyMode() {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	c.Args["x-queue-mode"] = "lazy" 
}

func DeclareQueue(channel *amqp.Channel, queueConfig RabbitMQQueueConfig) (string, error) {
	if channel == nil {
		return "", fmt.Errorf("could not create a queue: channel is nil")
	}

	var args amqp.Table
	if queueConfig.Args != nil {
		args = make(amqp.Table, len(queueConfig.Args))
		for key, value := range queueConfig.Args {
			args[key] = value
		}
	}

	q, err := channel.QueueDeclare(
		// if empty rabbit creates unique name by itself
		queueConfig.Name,
		queueConfig.IsDurable,
		queueConfig.DeleteWhenUnused,
		queueConfig.IsExclusive,
		queueConfig.IsNoWait,
		args,
	)

	if err != nil {
		return "", fmt.Errorf("could not create a rabbitmq queue \"%s\": %w", queueConfig.Name, err)
	}

	if len(q.Name) == 0 {
		return "", fmt.Errorf("could not initialise a rabbitmq queue: queue name is empty")
	}

	fmt.Printf("RabbitMQ queue \"%s\" was created\n", q.Name)

	return q.Name, nil
}
