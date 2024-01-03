# simple-rabbitmq-consumer-go
simple-rabbitmq-consumer-go

rabbit mq docker image 
https://hub.docker.com/_/rabbitmq

Tutorial how to work with rabbitmq
https://www.rabbitmq.com/tutorials/tutorial-one-go.html

Helpful args
https://habr.com/ru/articles/490960/

Messages retries
https://habr.com/ru/companies/domclick/articles/500978/

RabbitMQ can serve tens of thousands exhanges and queues without any priblems, but it demands node resources.

It is the main repo to maange access to RabbitMQ
go get github.com/rabbitmq/amqp091-go

By default consumer will collaborate with rabbitmq:3-alpine docker image
Here is example how run docker continer

docker run -d --rm --hostname my-rabbit -p 15672:15672 -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 25672:25672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password --name some-rabbit rabbitmq:3-alpine

We can use this command to connect to rabbitmq docker and manage or track it's conditions
docker exec -it <container name> sh

Get all queues
rabbitmqctl list_queues

Get all bindings 
rabbitmqctl list_bindings

Get all policies 
rabbitmqctl list_policies

Delete queue
rabbitmqctl delete_queue <queue name> 

Delete policy 
rabbitmqctl clear_policy <policy name> 

Create policy
rabbitmqctl set_policy Q1 "^queue" '{"dead-letter-routing-key":"queue1", "dead-letter-exchange":"queue1DeadLetterExchange"}' --apply-to queues
