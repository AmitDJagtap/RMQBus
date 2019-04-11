package RMQBus

import (
	"encoding/json"
	"log"
	"math/rand"
	"strings"

	"github.com/streadway/amqp"
)

var conn *amqp.Connection
var channel *amqp.Channel

func setup(rmquri string) {
	conn, err := amqp.Dial(rmquri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()
}

func rpc(topic string, msg string) interface{} {

	q, err := channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	err = channel.Publish(
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(msg),
		})
	failOnError(err, "Failed to publish a message")

	var res interface{}
	for d := range msgs {
		if corrId == d.CorrelationId {
			var req interface{}
			res = json.Unmarshal([]byte(d.Body), &req)
			failOnError(err, "Failed to convert body to json")
			break
		}
	}

	return res
}

func publish(topic string, msg string) {

	temp := strings.Split(topic, ".")
	exchange, rKey := temp[0], temp[1]

	err := channel.Publish(
		exchange, // exchange
		rKey,     // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})

	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent To %s", topic)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
