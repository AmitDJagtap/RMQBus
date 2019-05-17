package RMQBus

import (
	"encoding/json"
	"fmt"
	"log"
	"log-aggregator/utils"
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

type responder func(interface{}, chan interface{})

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
	value := os.Getenv("app")
	if len(value) == 0 {
		err1 := godotenv.Load()
		if err1 != nil {
			log.Panic(err1)
		}
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

func (RMQ *RMQ) InitFunctions(appName string, responderRegistry map[string]responder, consumerRegistry map[string]responder) {

	initCh, err := RMQ.Conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")

	err = initCh.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	utils.FailOnError(err, "Failed to set QoS")

	for k, responderFunction := range responderRegistry {

		key, resp := k, responderFunction
		temp := []string{"program.", key}
		topicName := strings.Join(temp, "")
		q, err := initCh.QueueDeclare(
			topicName, // name
			false,     // durable
			false,     // delete when usused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		utils.FailOnError(err, "Failed to declare a queue")

		msgs, err := initCh.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		utils.FailOnError(err, "Failed to register a consumer")
		go func() {
			for msgItem := range msgs {
				var req interface{}
				json.Unmarshal([]byte(msgItem.Body), &req)
				utils.FailOnError(err, "Failed to convert body to integer")

				//Call responder function
				cbFunc := make(chan interface{})
				go resp(req, cbFunc)
				response := <-cbFunc
				strResponse, err := json.Marshal(response)

				msgToSend := amqp.Publishing{
					DeliveryMode:  amqp.Persistent,
					ContentType:   "text/plain",
					CorrelationId: msgItem.CorrelationId,
					Body:          []byte(strResponse),
				}

				err = initCh.Publish("", msgItem.ReplyTo, false, false, msgToSend)
				utils.FailOnError(err, "Failed to publish a message")

				msgItem.Ack(true)
			}
		}()
		fmt.Println(" [x] Responder Registerd for event :", topicName)
	}

	initChConsumer, err := RMQ.Conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")

	for e, consumerinstance := range consumerRegistry {

		routingKey := e
		consumerFunction := consumerinstance
		temp := []string{"program.", routingKey}
		QueueName := strings.Join(temp, "")
		err = initChConsumer.ExchangeDeclare(
			appName,  // name
			"direct", // type
			false,    // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		utils.FailOnError(err, "Failed to declare an exchange")
		q, err := initChConsumer.QueueDeclare(
			QueueName, // name
			false,     // durable
			false,     // delete when usused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		utils.FailOnError(err, "Failed to declare a queue")

		err = initChConsumer.QueueBind(
			q.Name,     // queue name
			routingKey, // routing key
			appName,    // exchange
			false,
			nil)
		utils.FailOnError(err, "Failed to bind a queue")

		msgs, err := initChConsumer.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		utils.FailOnError(err, "Failed to register a consumer")
		go func() {
			for msgItem := range msgs {
				var req interface{}
				json.Unmarshal([]byte(msgItem.Body), &req)
				utils.FailOnError(err, "Failed to convert body")

				cbFunc := make(chan interface{})
				go consumerFunction(req, cbFunc)
				<-cbFunc

			}
		}()

		fmt.Println(" [x] Consumer registered for event :", routingKey)
	}
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
