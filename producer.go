package main

import (
	"bufio"
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strconv"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	version, err := sarama.ParseKafkaVersion("2.4.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config.Version = version
	brokerAddress := []string{"localhost:9092"}
	client, err := sarama.NewClient(brokerAddress, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close() //ignore error

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close() //ignore error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sentMsgs := 0

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-producer.Successes():
				sentMsgs += 1
				log.Println("sent ", sentMsgs, "messages")
			}
		}
	}(ctx)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-producer.Errors():

				log.Println(err)
			}
		}
	}(ctx)
	inputRecievedCh := make(chan int, 1)
	go func(ctx context.Context, inputRecievedCh chan int) {
		for {
			select {
			case integer := <-inputRecievedCh:
				log.Println(integer)
				for i := 1; i <= integer; i++ {
					msg := &sarama.ProducerMessage{Topic: "demo_topic", Key: nil, Value: sarama.StringEncoder(strconv.Itoa(i))}
					producer.Input() <- msg
				}
			case <-ctx.Done():
				return

			}
		}
	}(ctx, inputRecievedCh)

	for {
		stdReader := bufio.NewReader(os.Stdin)
		input, _ := stdReader.ReadString('\n')
		integer, err := strconv.Atoi(input[:len(input)-1])
		if err != nil {
			log.Println("please give an integer")
			continue
		}
		inputRecievedCh <- integer
	}
}
