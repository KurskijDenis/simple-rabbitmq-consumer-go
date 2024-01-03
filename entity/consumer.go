package entity

import (
	"context"
	"fmt"
	"os"

	// "time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ProcessMsgError struct {
	err        error
	isCritical bool
}

func (error ProcessMsgError) Error() string {
	return fmt.Sprintf("process msg error: %v", error.err)
}

func NewProcessMsgError(err error, isCritical bool) *ProcessMsgError {
	return &ProcessMsgError{err: err, isCritical: isCritical}
}

type RetryStratagy uint

const (
	AlwaysRetryStratagy RetryStratagy = iota
	DiscardRetryStratagy
	LimitedRetryStratagy
	RequeueRetryStratagy
)

type RabbitMQConsumerConfig struct {
	// Should be unique among all consumer. If empty rabbit generates name by itself.
	Name string
	// Queue name, where to connect.
	QueueName string
	// Auto confirm received messages.
	IsAutoAck bool
	// If true, only one consumer can consume from queue (if we create two consumer it would be a error)
	IsExclusive bool
	// When noWait is true, do not wait for the server to confirm the request and
	// immediately begin deliveries.  If it is not possible to consume, a channel
	// exception will be raised and the channel will be closed.
	IsNoWait bool
	// Stratage how resend msgs with errors
	RetryStratagy RetryStratagy
	// Max retry count (works only with limited retry strategy)
	MaxRetryCount uint
	// Prefetch msg count
	PrefetchMsgCount uint
	// Additional arguments for exchange
	Args map[string]any
}

func retryAlwaysMsg(delivery *amqp.Delivery) {
	if err := delivery.Nack(false, false); err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't send nack acknowledgement to rabbitmq:%v\n", err)
	}
}

func retryDiscardMsg(delivery *amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't send acknowledgement to rabbitmq:%v\n", err)
	}
	fmt.Fprintf(os.Stderr, "RabbitMQ message inevitably lost")
}

func retryRequeueMsg(delivery *amqp.Delivery) {
	if err := delivery.Nack(false, true); err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't send acknowledgement to rabbitmq(requeue):%v\n", err)
		fmt.Fprintf(os.Stderr, "RabbitMQ message inevitably lost")
	}
}

func retryLimitedMsg(delivery *amqp.Delivery, maxRetryCount uint) {

	if len(delivery.Headers) != 0 {
		if header, ok := delivery.Headers["x-death"]; ok {
			if deathInfoRoot, ok := header.([]any); ok {
				if deathInfo, ok := deathInfoRoot[0].(amqp.Table); ok {
					if count, ok := deathInfo["count"]; ok {
						if retryCount, ok := count.(int64); ok {
							fmt.Fprintf(os.Stdout, "Retry count is :%d\n", retryCount)
							if uint(retryCount) > maxRetryCount {

								if err := delivery.Ack(false); err != nil {
									fmt.Fprintf(os.Stderr, "Couldn't send acknowledgement to rabbitmq:%v\n", err)
								}

								fmt.Fprintf(os.Stderr, "RabbitMQ message exceded retry count and inevitable lost")
								return

							}
						} else {

							fmt.Fprintf(os.Stderr, "Can't parse dead letter retry count from headers")
						}
					} else {
						fmt.Fprintf(os.Stderr, "Can't parse dead letter retry counter")
					}
				}
			}
		}
	}

	if err := delivery.Nack(false, false); err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't send nack acknowledgement to rabbitmq:%v\n", err)
	}
}

type MsgHandler func(body []byte) *ProcessMsgError

func ConsumeMsgs(msgs <-chan amqp.Delivery, channel *amqp.Channel, retryStratagy RetryStratagy, maxRetryCount uint, handler MsgHandler) {
	for msg := range msgs {
		err := handler(msg.Body)
		if err == nil {
			if err := msg.Ack(false); err != nil {
				fmt.Fprintf(os.Stderr, "Couldn't send acknowledgement to rabbitmq:%v\n", err)
			}

			continue

		}
		if !err.isCritical {
			fmt.Fprintf(os.Stdout, "RabbitMQ message couldn't be consumed now: %v", err)
			switch retryStratagy {
			case AlwaysRetryStratagy:
				retryAlwaysMsg(&msg)
			case DiscardRetryStratagy:
				retryDiscardMsg(&msg)
			case LimitedRetryStratagy:
				retryLimitedMsg(&msg, maxRetryCount)
			case RequeueRetryStratagy:
				retryRequeueMsg(&msg)
			default:
				retryAlwaysMsg(&msg)
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't send acknowledgement to rabbitmq:%v\n", err)
		}
		fmt.Fprintf(os.Stderr, "RabbitMQ message inevitably lost: %v", err)
	}
}

func DeclareConsumer(
	ctx context.Context,
	channel *amqp.Channel,
	consumerConfig RabbitMQConsumerConfig,
	handler MsgHandler,
) error {
	if channel == nil {
		return fmt.Errorf("could not create a consumer delivery channel")
	}

	var args amqp.Table
	if consumerConfig.Args != nil {
		args = make(amqp.Table, len(consumerConfig.Args))
		for key, value := range consumerConfig.Args {
			args[key] = value
		}
	}

	if consumerConfig.PrefetchMsgCount != 0 {
		channel.Qos(int(consumerConfig.PrefetchMsgCount), 0, false)
	}

	deliveryChannel, err := channel.ConsumeWithContext(
		ctx,
		consumerConfig.QueueName,
		consumerConfig.Name,
		consumerConfig.IsAutoAck,
		consumerConfig.IsExclusive,
		// not supported in rabbit
		false,
		consumerConfig.IsNoWait,
		args,
	)

	if err != nil {
		return fmt.Errorf("could not create a consumer delivery channel: %w", err)
	}

	fmt.Println("Establish consumer delivery channel")

	ConsumeMsgs(deliveryChannel, channel, consumerConfig.RetryStratagy, consumerConfig.MaxRetryCount, handler)
	return nil
}
