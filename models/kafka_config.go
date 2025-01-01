package models

type Config struct {
	KAFKA_HOSTS             string       `json:"KAFKA_HOSTS"`
	KAFKA_TOPIC             string       `json:"KAFKA_TOPIC"`
	KAFKA_CONSUMER_GROUP_ID string       `json:"KAFKA_CONSUMER_GROUP_ID"`
	KAFKA_CONSUMER_OFFSET   string       `json:"KAFKA_CONSUMER_OFFSET"`
	KAFKA_SASL_USER         string       `json:"KAFKA_SASL_USERNAME"`
	KAFKA_SASL_PASS         string       `json:"KAFKA_SASL_PASSWORD"`
	KAFKA_SASL_MECHANISM    string       `json:"KAFKA_SASL_MECHANISM"`
	AZURE_CONFIGS           *AzureConfig `json:"AZURE_CONFIGS,omitempty"`
	AVRO_CONFIGS            *AvroConfig  `json:"AVRO_CONFIGS,omitempty"`
}
