package main

import (
	"fmt"
	"os"

	"github.com/KurskijDenis/simple-rabbitmq-consumer-go/entity"
	"github.com/KurskijDenis/simple-rabbitmq-consumer-go/example"
)

func main() {
	connectionConfig := entity.RabbitMQConnectionConfig{
		Url: "localhost",
		Port: 5672,
		User: "user",
		Password: "password",
	}
	connection, err := entity.CreateConnection(connectionConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Connection error: %v\n", err)
		return
	}
	fmt.Printf("Connation to rabbitmq was established\n")
	example.DeclareQueueWithDeadLetterExchange(connection)
	// Here two consumers get messages
	// example.DeclareQueueWithSeveralChannels(connection, false)
	// Here only one consumer get messages
	// example.DeclareQueueWithSeveralChannels(connection, true)
}
