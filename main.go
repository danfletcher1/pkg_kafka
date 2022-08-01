/*

Name: Dan Fletcher
Date: 23/04/18
Title: Kafka Streamer
Codebase:
Description:

To use the package you need to first set the connection string
it takes a host and messaging topic
then you can log message or errors

USAGE:

kafka.Connect("localhost")
go func() {
	kafka.Receive(conf["kafkaTopic"], receiveChannel)
		if err != nil {
		fmt.Println(err)
	}
}()

for {
	fmt.Println( <- receiveChannel)
}

kafka.Send("testtopic", "hello everyone")

*/

package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// Local variables
type Client struct {
	config   *sarama.Config
	server   []string
	producer sarama.AsyncProducer
}

// Connect to kafak. You must connect before any other function
func Connect(server []string) (c *Client, e error) {
	c = new(Client)
	c.server = server

	// Create Kafka connection strings
	c.config = sarama.NewConfig()
	c.config.Net.WriteTimeout = time.Duration(5 * time.Second)
	c.config.Net.DialTimeout = time.Duration(5 * time.Second)
	c.config.Net.WriteTimeout = time.Duration(5 * time.Second)
	c.config.Net.KeepAlive = time.Duration(5 * time.Second)
	c.config.Producer.Return.Successes = true
	c.config.Producer.Return.Errors = true
	c.config.Producer.Compression = sarama.CompressionSnappy // Compress messages using snappy
	//config.Producer.CompressionLevel =  // don't know the compression options leaving to default
	c.config.Producer.Flush.Frequency = time.Millisecond * 100 // Bunches messages and max send every ....
	//config.Producer.Flush.Bytes = 1000 // Bunch message and send when message size is maximum of ....
	// Don't go over 1Mb without updating the consumers default on the server ....

	// Connect to produce on Kafka just error and return, dont want the main program failing
	c.producer, e = sarama.NewAsyncProducer(server, c.config)
	if e != nil {
		return nil, errors.New("Failed to connect to kafka server. " + e.Error())
	}
	return c, nil
}

// Receive will output messages from a kafka topic as they arrive on the channel provided
// run as a goroutine, the function will hang waiting for messages, and exits with <-signals
func (c *Client) Receive(topic string, receiveChannel chan []byte) error {

	// Connect to consume Kafka just error and return, don't want the main program failing
	consumer, err := sarama.NewConsumer(c.server, c.config)
	if err != nil {
		return errors.New("Failed to open Kafka consumer " + topic + ". " + err.Error())
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			errors.New("ERROR: Failed to close Kafka producer. " + err.Error())
		}
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return errors.New("ERROR: Failed to find a list of kafka partitions for this topic. " + err.Error())
	}

	// Start a listener for all partitions
	for _, partition := range partitions {
		go func(partition int32) {

			// Connect to partition
			fmt.Println("Listening to Kafka topic", topic, "partition", partition)
			partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				errors.New("ERROR: Failed to open Kafka consumer partition. " + err.Error())
			}

			for {
				select {
				// We will receive the message
				case msg := <-partitionConsumer.Messages():
					receiveChannel <- msg.Value
					// If there is a terminate signal
				}

			}
			// Close partition
			defer func() {
				if err := partitionConsumer.Close(); err != nil {
					errors.New("ERROR: Failed to close Kafka consumer partition. " + err.Error())
				}

			}()
		}(partition)

	}

	// End operation but don't close
	signals := make(chan bool)
	<-signals

	return nil
}

// Close should be run as a defered process right after connect
func (c *Client) Close() error {
	if err := c.producer.Close(); err != nil {
		return errors.New("Failed to disconnect Kafka producer. " + err.Error())
	}
	return nil
}

// Send any message to the kafka topic specified
func (c *Client) Send(topic, msg string) error {
	c.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msg)}

	// we are doing a wait here for a reply
	select {
	case <-c.producer.Successes():
		return nil
	case err := <-c.producer.Errors():
		return errors.New("Failed to send kafka message to " + topic + ". " + err.Error())
	}
}
