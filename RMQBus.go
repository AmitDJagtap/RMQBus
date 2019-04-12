package RMQBus

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type RMQ struct {
	Ch   *amqp.Channel
	Conn *amqp.Connection
}

var singleton *RMQ
var once sync.Once

func GetConnection() *RMQ {
	once.Do(func() {

		conn, err := amqp.Dial(os.Getenv("rmq_uri"))
		failOnError(err, "Failed to connect to RabbitMQ")

		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")

		singleton = &RMQ{Ch: ch, Conn: conn}
	})
	return singleton
}

func init() {
	err1 := godotenv.Load()
	if err1 != nil {
		log.Panic(err1)
	}
}

func (RMQ *RMQ) Rpc(topic string, msg string) interface{} {

	ch, err := RMQ.Conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
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

	err = ch.Publish(
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

func (RMQ *RMQ) Publish(topic string, msg string) {

	temp := strings.Split(topic, ".")
	exchange, rKey := temp[0], temp[1]

	ch, err := RMQ.Conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.Publish(
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
