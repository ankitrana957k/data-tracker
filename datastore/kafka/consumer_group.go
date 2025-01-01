package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/krogertechnology/data-tracker/models"
)

type ConsumerHandler struct {
	ChannelMap map[string]chan models.Message
}

func (c *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	log := "Successfully setup consumer group session\n"
	for k := range c.ChannelMap {
		c.ChannelMap[k] <- models.Message{Logs: log}
	}

	return nil
}

func (c *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log := "Exiting consumer group session"
	for k := range c.ChannelMap {
		c.ChannelMap[k] <- models.Message{Logs: log}
	}

	return nil
}

func (c *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		headers := convertSaramaHeaderToMap(msg.Headers)
		data := models.Message{
			Headers:   headers,
			Offset:    msg.Offset,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Value:     msg.Value,
			Logs:      fmt.Sprintf("Consumed from topic %v, Partition %v with Offset %v\n", msg.Topic, msg.Partition, msg.Offset),
		}

		c.ChannelMap[msg.Topic] <- data
		session.MarkMessage(msg, "")
	}

	return nil
}

func (c *ConsumerHandler) Rebalance(session sarama.ConsumerGroupSession, old []string, new []string) {
	fmt.Printf("Rebalancing group from %v to %v\n", old, new)
}
