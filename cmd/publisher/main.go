package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/KurskijDenis/simple-rabbitmq-consumer-go/entity"
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

	channel, err := connection.CreateNewChannel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Channel error: %v\n", err)
		return
	}

	publisher, err := entity.NewPublisher(
		channel,
		entity.RabbitMQPublisherConfig{
			IsMandatory: true,
			IsImmediate: false,
			IsPersistent: true,
			SendStrategy: entity.SkipSendStrategy,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't create publisher: %v\n", err)
	}
	ctx := context.Background()
	startId := 1

	for {
		structToSend := struct{
			Id int `json:"id"`
			Text string `json:"txt"`
		} {}
		structToSend.Id = startId
		structToSend.Text = "Hi"
		b, _ := json.Marshal(structToSend)

		time.Sleep(time.Second)
		publisher.Publish(ctx, "", "queue1", "text/plain", b, strconv.Itoa(startId))
		startId = startId + 1
		fmt.Fprintf(os.Stdout, "publish new msg\n")
	}
}
