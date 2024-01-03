package example

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/KurskijDenis/simple-rabbitmq-consumer-go/entity"
)

func ProcessMsg(body []byte) *entity.ProcessMsgError {
	var jsonMsg any
	if err := json.Unmarshal(body, &jsonMsg); err != nil {
		return entity.NewProcessMsgError(err, true)
	}

	fmt.Printf("Json %v\n", jsonMsg)
	return entity.NewProcessMsgError(errors.New("Special error to check attempts"), false)
}

func DeclareQueueWithDeadLetterExchange(connection entity.RabbitMQConnection) {
	// Channel use to send messages to RabbitMQ, if we have several readers we can create several connections.
	// We should ack or nack msg by using the same channel
	channel, err := connection.CreateNewChannel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Channel error: %v\n", err)
		return
	}
	fmt.Printf("Channel to rabbitmq was created\n")
	///////////////////////////////////////////////////////////////////
	queue1Config := entity.RabbitMQQueueConfig{
		Name:             "queue1",
		IsDurable:        true,
		DeleteWhenUnused: false,
		IsExclusive:      false,
		IsNoWait:         false,
	}
	queue1Config.SetDeadLetterExchange("queue1DeadLetterExchange")
	queue1Config.SetDeadLetterKey("retryQueue1")
	//queue1Config.SetMessageTTL(time.Hour)
	// Decalre the main queue to extract messages from it. It should be declared the declared the same way.
	// If we want to use more flexible way, we can use policies in rabbit MQ. We can change policies on server
	// and don't care how to declare it.
	queue1Name, err := entity.DeclareQueue(channel.Channel, queue1Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare queue with name %s: %v\n", queue1Config.Name, err)
		return
	}
	fmt.Printf("Queue %s was created\n", queue1Name)
	///////////////////////////////////////////////////////////////////
	queue2Config := entity.RabbitMQQueueConfig{
		Name:             "retryQueue1",
		IsDurable:        true,
		DeleteWhenUnused: false,
		IsExclusive:      false,
		IsNoWait:         false,
	}
	queue2Config.SetDeadLetterExchange("")
	queue2Config.SetMessageTTL(3 * time.Second)
	queue2Config.SetDeadLetterKey("queue1")
	// Queue to get dead letters and send it again
	queue2Name, err := entity.DeclareQueue(channel.Channel, queue2Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare queue with name %s: %v", queue2Config.Name, err)
		return
	}
	fmt.Printf("Queue %s was created\n", queue2Name)

	if err := entity.DeclareExchange(
		channel.Channel,
		entity.RabbitMQExchangeConfig{
			Name:             "queue1DeadLetterExchange",
			Kind:             entity.DirectExchangeType,
			IsDurable:        true,
			DeleteWhenUnused: true,
			IsNoWait:         false,
		},
	); err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare exchange with name %s: %v", "queue1DeadLetterExchange", err)
		return
	}
	fmt.Printf("Exchange %s was created\n", "queue1DeadLetterExchange")

	// Create exchage to forward messages to dead letter queue
	if err := entity.DeclareExchange(
		channel.Channel,
		entity.RabbitMQExchangeConfig{
			Name:             "queue1RetryExchange",
			Kind:             entity.DirectExchangeType,
			IsDurable:        true,
			DeleteWhenUnused: true,
			IsNoWait:         false,
		},
	); err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare exchange with name %s: %v", "queue1RetryExchange", err)
		return
	}
	fmt.Printf("Exchange %s was created\n", "queue1RetryExchange")

	// Declare binding between exchange and dead letter queue
	if err := entity.DeclareBindings(
		channel.Channel,
		entity.RabbitMQBindingConfig{
			ExchangeName: "queue1DeadLetterExchange",
			QueueName:    "retryQueue1",
			Key:          "retryQueue1",
		},
	); err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare binding: %v", err)
		return
	}

	// Declare binding between exchange and main queue
	if err := entity.DeclareBindings(
		channel.Channel,
		entity.RabbitMQBindingConfig{
			ExchangeName: "queue1RetryExchange",
			QueueName:    "queue1",
			Key:          "queue1",
		},
	); err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare binding: %v", err)
		return
	}
	fmt.Println("Start declare consumer")

	ctx := context.Background()
	// Declare consumer from main queue
	err = entity.DeclareConsumer(
		ctx,
		channel.Channel,
		entity.RabbitMQConsumerConfig{
			Name:             "queueConsumer1",
			QueueName:        "queue1",
			IsAutoAck:        false,
			IsExclusive:      false,
			RetryStratagy:    entity.LimitedRetryStratagy,
			MaxRetryCount:    3,
			PrefetchMsgCount: 5,
		},
		ProcessMsg,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't create consumer: %v", err)
		return
	}
}
