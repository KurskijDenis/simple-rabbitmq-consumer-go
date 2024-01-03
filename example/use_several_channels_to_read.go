package example

import "github.com/KurskijDenis/simple-rabbitmq-consumer-go/entity"
import "encoding/json"
import "fmt"
import "os"
import "time"
import "context"

func ReturnProcessMsgUseSeveralChannels(consumerName string) func(body []byte) *entity.ProcessMsgError {
	return func(body []byte) *entity.ProcessMsgError {
		var jsonMsg any
		if err := json.Unmarshal(body, &jsonMsg); err != nil {
			return entity.NewProcessMsgError(err, true)
		}

		fmt.Printf("Json %v from consumer %s\n", jsonMsg, consumerName)
		return nil
	}
}

func DeclareConsumerUseSeveralChannels(ctx context.Context, channel entity.RabbitMQChannel, consumerName string, consumerIsExclusive bool) {
	err := entity.DeclareConsumer(
		ctx,
		channel.Channel,
		entity.RabbitMQConsumerConfig{
			Name:             consumerName,
			QueueName:        "queue1",
			IsAutoAck:        false,
			IsExclusive:      consumerIsExclusive,
			RetryStratagy:    entity.LimitedRetryStratagy,
			MaxRetryCount:    3,
			PrefetchMsgCount: 5,
		},
		ReturnProcessMsgUseSeveralChannels(consumerName),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't create consumer: %v", err)
		return
	}
}

func DeclareQueueWithSeveralChannels(connection entity.RabbitMQConnection, consumerIsExclusive bool) {
	// Channel use to send messages to RabbitMQ, if we have several readers we can create several connections.
	// We should ack or nack msg by using the same channel
	channel1, err := connection.CreateNewChannel()
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
	if consumerIsExclusive {
		queue1Config.SetSingleActiveConusmer()
	}
	//queue1Config.SetMessageTTL(time.Hour)
	// Decalre the main queue to extract messages from it. It should be declared the declared the same way.
	// If we want to use more flexible way, we can use policies in rabbit MQ. We can change policies on server
	// and don't care how to declare it.
	queue1Name, err := entity.DeclareQueue(channel1.Channel, queue1Config)
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
	queue2Name, err := entity.DeclareQueue(channel1.Channel, queue2Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't declare queue with name %s: %v", queue2Config.Name, err)
		return
	}
	fmt.Printf("Queue %s was created\n", queue2Name)

	if err := entity.DeclareExchange(
		channel1.Channel,
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
		channel1.Channel,
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
		channel1.Channel,
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
		channel1.Channel,
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

	go DeclareConsumerUseSeveralChannels(ctx, channel1, "queueConsumer1", false)
	channel2, err := connection.CreateNewChannel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Channel 2 error: %v\n", err)
		return
	}
	fmt.Printf("Channel 2 to rabbitmq was created\n")
	DeclareConsumerUseSeveralChannels(ctx, channel2, "queueConsumer2", false)
}
