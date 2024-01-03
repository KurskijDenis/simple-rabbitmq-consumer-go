package entity

import (
	"context"
	"errors"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SendStrategy uint

const (
	// Send new msg after confiramtion
	SyncSendStrategy SendStrategy = iota
	// Send new msg without confirmation
	AsyncSendStrategy
	// Don't care about msg just send it and forget sending result
	SkipSendStrategy
)

const (
	defaultAcknowledgeBufferSize = 5
)

type RabbitMQPublisherConfig struct {
	// If true, msg is successfully sent if we save it in durable queue
	IsPersistent bool
	// if no queue where msg should be preserved NotifyReturn would be called
	IsMandatory bool
	// if no conusmer where msg should be sent NotifyReturn would be called
	IsImmediate bool
	// How to process send errors
	SendStrategy SendStrategy
	// Default buffer size to acknoledge sending result
	AcknowledgeBufferSize uint
}

type Publisher struct {
	config          RabbitMQPublisherConfig
	rabbitMQChannel RabbitMQChannel
	// Chan to sync send messagges
	confirmChannel chan amqp.Confirmation
}

func (publisher *Publisher) Publish(
	ctx context.Context,
	exchangeName string,
	routingKey string,
	contentType string,
	body []byte,
	msgID string,
) error {
	// TODO fix it silly solution
	deliveryMode := amqp.Persistent
	if !publisher.config.IsPersistent {
		deliveryMode = amqp.Transient
	}

	err := publisher.rabbitMQChannel.Channel.PublishWithContext(
		ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		publisher.config.IsMandatory,
		publisher.config.IsImmediate,
		amqp.Publishing{
			DeliveryMode: deliveryMode,
			ContentType:  contentType,
			Body:         body,
			MessageId:    msgID,
		})
	if err != nil {
		return err
	}

	if publisher.confirmChannel != nil {
		select {
		case conf := <-publisher.confirmChannel:
			if conf.Ack {
				fmt.Fprintf(os.Stdout, "msg was delivered %d\n", conf.DeliveryTag)
			} else {
				return errors.New("Can't send message to rabbitMQ: message was nacked")
			}
		}
	}

	return nil
}

func NewPublisher(channel RabbitMQChannel, config RabbitMQPublisherConfig) (Publisher, error) {
	if config.SendStrategy != SkipSendStrategy {
		if err := channel.Channel.Confirm(false); err != nil {
			return Publisher{}, fmt.Errorf("channel can't confirm messages: %w", err)
		}
	}

	if config.AcknowledgeBufferSize == 0 {
		config.AcknowledgeBufferSize = defaultAcknowledgeBufferSize
	}

	var confirmChannel chan amqp.Confirmation
	switch config.SendStrategy {
	case AsyncSendStrategy:
		go func() {

			np_channel := channel.Channel.NotifyPublish(
				make(chan amqp.Confirmation, config.AcknowledgeBufferSize))
			for {
				select {
				case conf := <-np_channel:
					if conf.Ack {
						fmt.Fprintf(os.Stdout, "msg was delivered %d\n", conf.DeliveryTag)
					} else {
						fmt.Fprintf(os.Stderr, "got error couldn't send msg with tag %d\n", conf.DeliveryTag)
					}
				}
			}
		}()
	case SyncSendStrategy:
		confirmChannel = channel.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	if config.SendStrategy != SkipSendStrategy && (config.IsMandatory || config.IsImmediate) {
		go func() {

			nr_channel := channel.Channel.NotifyReturn(
				make(chan amqp.Return, config.AcknowledgeBufferSize))
			for {
				select {
				case conf := <-nr_channel:
					fmt.Fprintf(os.Stderr, "got error couldn't promote msg %s\n", string(conf.Body))
				}
			}
		}()

	}
	return Publisher{config: config, rabbitMQChannel: channel, confirmChannel: confirmChannel}, nil
}
