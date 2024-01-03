package entity

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeType uint16

const (
	// Send messages to all queue and doesn't depend on binding key
	FanoutExchangeType ExchangeType = iota
	// Send messages to queues with the same kay as key in binding between queue and exchange 
	DirectExchangeType
	// Send messages to queues which satisfies topic creterin key(regexp) * - any word, # - any set of words
	TopicExchangeType
	// Send messages to queues which satisfies headers value ("x-match": "any") satisfy only one header, ("x-match": "all") satisfy all headers
	HeadersExchangeType
	// Also rabbitmq(not amqp) has Exchange to Exchange route, and we can set this link by arguments 
)

type RabbitMQExchangeConfig struct {
	// Exchange name (use to bind with queues)
	Name string
	// Exchange type by default it is fanout 
	Kind ExchangeType 
	// Has to rabbbitmq save exchanges on filesystem and up them if crush happend
	IsDurable bool 
	// Delete exchange if no one queues are binded with exchange 
	DeleteWhenUnused bool 
	// Controversal param some sources say it is deprecated (but in general it uses for exchange to exchange bind)  
	Internal bool 
	// No wait echange creation response, if something went wrong, it would raise exception channel
	IsNoWait bool 
	// Additional arguments for exchange
	Args map[string]any
}

func DeclareExchange(channel *amqp.Channel, exchangeConfig RabbitMQExchangeConfig) error {
	if channel == nil {
		return fmt.Errorf("could not create a exchange: channel is nil")
	}

	var args amqp.Table
	if exchangeConfig.Args != nil {
		args = make(amqp.Table, len(exchangeConfig.Args))
		for key, value := range exchangeConfig.Args {
			args[key] = value
		}
	}

	exchangeKindNames := map[ExchangeType] string {
	    FanoutExchangeType: "fanout",
	    DirectExchangeType: "direct",
	    TopicExchangeType: "topic",
	    HeadersExchangeType: "headers",
	}
	exchangeKind := exchangeKindNames[exchangeConfig.Kind]

	err := channel.ExchangeDeclare(
		exchangeConfig.Name,
		exchangeKind,
		exchangeConfig.IsDurable,
		exchangeConfig.DeleteWhenUnused,
		exchangeConfig.Internal,
		exchangeConfig.IsNoWait,
		args,
	)

	if err != nil {
		return fmt.Errorf("could not create a rabbitmq exchange \"%s\": %w", exchangeConfig.Name, err)
	}

	fmt.Printf("RabbitMQ exchange \"%s\" was created\n", exchangeConfig.Name)

	return nil
}
