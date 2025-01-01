package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/krogertechnology/data-tracker/models"
	"github.com/krogertechnology/data-tracker/utils"
)

type KafkaConfig struct {
	KAFKA_HOSTS             string   `json:"KAFKA_HOSTS"`
	KAFKA_TOPIC             string   `json:"KAFKA_TOPIC"`
	KAFKA_CONSUMER_GROUP_ID string   `json:"KAFKA_CONSUMER_GROUP_ID"`
	KAFKA_CONSUMER_OFFSET   string   `json:"KAFKA_CONSUMER_OFFSET"`
	KAFKA_SASL_USER         string   `json:"KAFKA_SASL_USER"`
	KAFKA_SASL_PASS         string   `json:"KAFKA_SASL_PASS"`
	KAFKA_SASL_MECHANISM    string   `json:"KAFKA_SASL_MECHANISM"` // We have enabled the SASL authentication (User, Password)
	OFFSET                  int64    // This is the sarama offset value will be populated automatically
	TOPICS                  []string // This will be populated automatically
	*models.AzureConfig
	*AvroConfig
}

type KafkaConsumer struct {
	sarama.Consumer
	sarama.ConsumerGroup
	Configs *KafkaConfig
}

func (k *KafkaConfig) EstablishKafkaConn() (sarama.Client, error) {
	config := sarama.NewConfig()

	config.Net.TLS.Enable = true
	config.Consumer.Return.Errors = true
	config.Net.SASL.Enable = true

	// Consumer group keeps on rebalancing
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	switch k.KAFKA_SASL_MECHANISM {
	case "PLAIN":
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = k.KAFKA_SASL_USER
		config.Net.SASL.Password = k.KAFKA_SASL_PASS

	case "OUTHBEARER":
		config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		config.Net.SASL.TokenProvider = NewTokenProvider(k)
		config.Net.SASL.Version = sarama.SASLHandshakeV0

		if k == nil {
			return nil, errors.New("azure Configs are not provided")
		}

		if k.AAD_APPLICATION_ID == "" || k.AAD_APPLICATION_SECRET == "" || k.AAD_TENANT_ID == "" || k.AAD_AUDIENCE == "" {
			return nil, errors.New("missing azure configs")
		}

	case "PLAINTEXT":
		config.Net.SASL.Enable = false
		config.Net.TLS.Enable = false

	default:
		return nil, errors.New("unsupported SASL mechanism")
	}

	config.Consumer.Group.Member.UserData = []byte(k.KAFKA_CONSUMER_GROUP_ID)

	config.ClientID = "sarama"
	config.Version = sarama.V2_0_0_0

	client, err := sarama.NewClient([]string{k.KAFKA_HOSTS}, config)
	if err != nil {
		return nil, err
	}

	err = k.validateTopicAndOffset(client)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func CreateConsumerGroup(client sarama.Client, groupID string) (sarama.ConsumerGroup, error) {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}

	return consumerGroup, nil
}

func CreateConsumer(client sarama.Client) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (k *KafkaConfig) validateTopicAndOffset(client sarama.Client) error {
	var offset int64

	// Get a list of topics present in the cluster
	activeTopics, err := client.Topics()
	if err != nil {
		return err
	}

	// Create a map of active topics for quick lookup
	m := make(map[string]bool, 0)
	for i := range activeTopics {
		m[activeTopics[i]] = true
	}

	// Multiple topics could be provide in the configs
	consumeFromTopics := utils.GetElementsFromString(k.KAFKA_TOPIC)

	// We can only consume from one topic at a time with SASL Protocol
	// since the SASL Password will be different hence a new client will be required.
	for i := range consumeFromTopics {
		_, ok := m[consumeFromTopics[i]]
		if !ok {
			return errors.New("The topic " + consumeFromTopics[i] + " does not exist in this cluster.")
		}
	}

	offsetVal := strings.ToUpper(k.KAFKA_CONSUMER_OFFSET)

	switch offsetVal {
	case "OLDEST", "EARLIEST":
		offset = sarama.OffsetOldest
	case "LATEST":
		offset = sarama.OffsetNewest
	default:
		return fmt.Errorf("error invalid offset: %v", k.KAFKA_CONSUMER_OFFSET)
	}

	k.OFFSET = offset

	return nil
}

func (k *KafkaConsumer) ReadFromConsumer(channelMap map[string]chan models.Message) error {
	topic := k.Configs.KAFKA_TOPIC
	offset := k.Configs.OFFSET

	partitions, err := k.Consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("No partitions found in the cluster: " + err.Error())
	}

	var wg sync.WaitGroup
	for j := range partitions {
		wg.Add(1)
		pc, err := k.Consumer.ConsumePartition(topic, partitions[j], offset)
		if err != nil {
			return fmt.Errorf("Couldn't consume partition due to error: " + err.Error())
		}

		go func(j int) {
			defer wg.Done()

			log := fmt.Sprintf("Started consuming from partition[%d]\n", partitions[j])
			channelMap[k.Configs.TOPICS[0]] <- models.Message{Logs: log}

			ConsumeMessages(pc, channelMap)

		}(j)
	}

	wg.Wait()

	return nil
}

func (k *KafkaConsumer) ReadFromConsumerGroup(channelMap map[string]chan models.Message) error {
	topics := k.Configs.TOPICS

	for {
		err := k.ConsumerGroup.Consume(context.Background(), topics, &ConsumerHandler{channelMap})
		if err != nil {
			return err
		}
	}
}

func ConsumeMessages(pc sarama.PartitionConsumer, channelMap map[string]chan models.Message) {
	for msg := range pc.Messages() {
		headers := convertSaramaHeaderToMap(msg.Headers)
		data := models.Message{
			Headers:   headers,
			Offset:    msg.Offset,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Value:     msg.Value,
			Logs:      fmt.Sprintf("Consumed from topic %v, Partition %v with Offset %v\n", msg.Topic, msg.Partition, msg.Offset),
		}

		channelMap[msg.Topic] <- data
	}
}

func convertSaramaHeaderToMap(headers []*sarama.RecordHeader) map[string]string {
	convertedHeaders := make(map[string]string, 0)

	for _, header := range headers {
		key := string(header.Key)
		value := string(header.Value)

		convertedHeaders[key] = value
	}

	return convertedHeaders
}
