package entity

import (
	"fmt"

	"golang.org/x/exp/constraints"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConnectionConfig struct {
	// RabbitMQ url
	Url string
	// RabbitMQ port 
	Port uint16
	// RabbitMQ user name
	User string
	// RabbitMQ password 
	// Of course it is unsafe, but this program only for test
	Password string
}

type RabbitMQChannel struct {
	Channel *amqp.Channel
}

type RabbitMQConnection struct {
	Config RabbitMQConnectionConfig
	channels []RabbitMQChannel
	connection *amqp.Connection
}

func (connection* RabbitMQConnection) CreateNewChannel() (RabbitMQChannel, error) {
	if connection.connection == nil {
		return RabbitMQChannel{},  fmt.Errorf("could not create a rabbitmq channel: connection is nil")
	}
	ch, err := connection.connection.Channel()
	if err != nil {
		return RabbitMQChannel{}, fmt.Errorf("could not create a rabbitmq channel: %w", err)
	}
	if ch == nil {
		return RabbitMQChannel{}, fmt.Errorf("could not initialise a rabbitmq channel: amqp channel is nil")

	}
	newChannel := RabbitMQChannel{Channel: ch}
	connection.channels = append(connection.channels, newChannel)
	return newChannel, nil
}

func GetOptionalString(value string, defaultValue string) string {
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

func GetOptionalUIntValue[T constraints.Unsigned](value T, defaultValue T) T{
	if uint64(value) == 0 {
		return defaultValue
	}
	return value
}


func (c RabbitMQConnectionConfig) getFullUrl() string {
	user := GetOptionalString(c.User, "guest")
	psw := GetOptionalString(c.Password, "guest")
	url := GetOptionalString(c.Url, "localhost")
	port := GetOptionalUIntValue(c.Port, uint16(15762))

	return fmt.Sprintf("amqp://%s:%s@%s:%d/", user, psw, url, port)
}

func CreateConnection(conf RabbitMQConnectionConfig) (RabbitMQConnection, error) {
	fmt.Println(conf.getFullUrl())
	connection, err := amqp.Dial(conf.getFullUrl())
	if err != nil {
		return RabbitMQConnection{}, fmt.Errorf("could not initialise a connection to rabbitmq: %w", err)
	}

	if connection == nil {
		return RabbitMQConnection{}, fmt.Errorf("could not initialise a connection to rabbitmq: amqp connection is nil")

	}

	rabbitmqConnection := RabbitMQConnection{
		Config: conf,
		connection: connection,
	}

	return rabbitmqConnection, nil

}

// We can create several channels and they will be like different clients
func CreateChannel(connection *amqp.Connection) (*RabbitMQChannel, error) {
	if connection == nil {
		return nil,  fmt.Errorf("could not create a rabbitmq channel: connection is nil")
	}
	ch, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("could not create a rabbitmq channel: %w", err)
	}
	if ch == nil {
		return nil, fmt.Errorf("could not initialise a rabbitmq channel: amqp channel is nil")

	}
	return &RabbitMQChannel{Channel: ch}, nil
}
