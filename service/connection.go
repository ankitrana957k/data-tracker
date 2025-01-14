package service

import (
	"fmt"
	"sync"

	"fyne.io/fyne/v2/widget"
	"github.com/IBM/sarama"

	datastore "github.com/krogertechnology/data-tracker/datastore/kafka"
	"github.com/krogertechnology/data-tracker/models"
	"github.com/krogertechnology/data-tracker/utils"
)

type KafkaOBJ struct {
	Configs       datastore.KafkaConfig
	DataChannel   map[string]chan models.Message
	ClearMsgAfter int
}

func NewKafkaObj(k models.Config, clearMsgAfter int) *KafkaOBJ {
	topics := utils.GetElementsFromString(k.KAFKA_TOPIC)

	config := datastore.KafkaConfig{
		KAFKA_HOSTS:             k.KAFKA_HOSTS,
		KAFKA_TOPIC:             k.KAFKA_TOPIC,
		KAFKA_CONSUMER_GROUP_ID: k.KAFKA_CONSUMER_GROUP_ID,
		KAFKA_CONSUMER_OFFSET:   k.KAFKA_CONSUMER_OFFSET,
		TOPICS:                  topics,
		KAFKA_SASL_USER:         k.KAFKA_SASL_USER,
		KAFKA_SASL_PASS:         k.KAFKA_SASL_PASS,
		KAFKA_SASL_MECHANISM:    k.KAFKA_SASL_MECHANISM,
		AzureConfig:             k.AZURE_CONFIGS,
		AvroConfig:              (*datastore.AvroConfig)(k.AVRO_CONFIGS),
	}

	return &KafkaOBJ{
		Configs:       config,
		ClearMsgAfter: clearMsgAfter,
		DataChannel:   make(map[string]chan models.Message),
	}
}

func (k *KafkaOBJ) SetupEventhub() (sarama.Client, error) {
	client, err := k.Configs.EstablishKafkaConn()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (k *KafkaOBJ) Read(client sarama.Client) error {
	kafkaConsumer := datastore.KafkaConsumer{
		Configs: &k.Configs,
	}

	if len(k.Configs.TOPICS) == 1 {
		consumer, err := datastore.CreateConsumer(client)
		if err != nil {
			return err
		}

		defer consumer.Close()

		kafkaConsumer.Consumer = consumer

		err = kafkaConsumer.ReadFromConsumer(k.DataChannel)
		if err != nil {
			return err
		}

	} else {
		consumerGroup, err := datastore.CreateConsumerGroup(client, k.Configs.KAFKA_CONSUMER_GROUP_ID)
		if err != nil {
			return err
		}

		defer consumerGroup.Close()

		kafkaConsumer.ConsumerGroup = consumerGroup

		err = kafkaConsumer.ReadFromConsumerGroup(k.DataChannel)
		if err != nil {
			return err
		}
	}

	return nil
}

func (k *KafkaOBJ) Listen(widgetMap map[string]*widget.Label) error {
	var wg sync.WaitGroup

	for topic := range k.DataChannel {
		wg.Add(1)

		go func(topic string) error {
			defer wg.Done()

			err := k.ConsumeAndOverwriteText(topic, widgetMap[topic])
			if err != nil {
				return fmt.Errorf("error displaying data for topic %s: %v", topic, err)
			}

			return nil

		}(topic)
	}

	wg.Wait()

	return nil
}

func (k *KafkaOBJ) ConsumeAndOverwriteText(topic string, textWidget *widget.Label) error {
	channel := k.DataChannel[topic]
	count := 1

	for message := range channel {
		var (
			data = message.Value
			err  error
			info string
		)

		// Clear Existing Messages
		if count > k.ClearMsgAfter {
			textWidget.SetText("")
		}

		if data == nil {
			// LOG
			info = textWidget.Text + message.Logs
			textWidget.SetText(info)
			continue
		}

		if !utils.IsJSON(message.Value) {
			val, err := k.Configs.ProcessAvroMessage(message)
			if err != nil {
				return err
			}

			val.Logs = message.Logs
			message = *val
		}

		data, err = utils.FormatJSONString(message.Value)
		if err != nil {
			return fmt.Errorf("error formatting JSON: %v", err)
		}

		info = message.Logs + string(data) + "\n"
		count += 1

		if textWidget.Text != "" {
			textWidget.SetText(textWidget.Text + "\n" + info)
		} else {
			textWidget.SetText(info)
		}
	}

	return nil
}
