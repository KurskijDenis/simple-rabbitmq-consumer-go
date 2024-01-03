package entity 

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HeaderRoutingType uint8

const (
	AnyHeaderRoutingType HeaderRoutingType = iota 
	AllHeaderRoutingType
)

type RabbitMQBindingConfig struct {
	// Exchange name
	ExchangeName string
	// Queue name
	QueueName string
	// Kay which connects exchange and queue. If exchange fanuot or header(TODO check header) it is redudant value.
	// It has power if we talk about topic and direct echange types.
	Key string
	// No wait binding creation response, if something went wrong, it would raise exception in channel
	IsNoWait bool 
	// Additional arguments for binding
	Args map[string]any
}

type BindingHeader struct {
	Key string
	Value string
}

func (c *RabbitMQBindingConfig) SetHeaderRouting(routingType HeaderRoutingType, headers ...BindingHeader) error {
	if c.Args == nil {
		c.Args = make(map[string]any)
	}
	switch routingType {
	case AnyHeaderRoutingType: c.Args["x-match"] = "any"
	case AllHeaderRoutingType: c.Args["x-match"] = "all"
	default:
		return fmt.Errorf("unexpected header routing type %d", routingType)
	}
	for _, v := range headers {
		c.Args[v.Key] = v.Value
	}
	return nil
}

func DeclareBindings(channel *amqp.Channel, bindingConfig RabbitMQBindingConfig) error {
	if channel == nil {
		return fmt.Errorf("could not create a exchange: channel is nil")
	}

	var args amqp.Table
	if bindingConfig.Args != nil {
		args = make(amqp.Table, len(bindingConfig.Args))
		for key, value := range bindingConfig.Args {
			args[key] = value
		}
	}

	err := channel.QueueBind(
		bindingConfig.QueueName,
		bindingConfig.Key,
		bindingConfig.ExchangeName,
		bindingConfig.IsNoWait,
		args,
	)

	if err != nil {
		return fmt.Errorf(
			"could not bind echange \"%s\" and queue \"%s\": %w",
			bindingConfig.ExchangeName,
			bindingConfig.QueueName,
			err)
	}

	fmt.Printf(
		"RabbitMQ binding echange \"%s\" and queue \"%s\" was established\n",
		bindingConfig.ExchangeName,
		bindingConfig.QueueName)

	return nil
}
