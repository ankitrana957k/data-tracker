package models

type AvroConfig struct {
	SCHEMA_URL     string `json:"AVRO_SCHEMA_URL,omitempty"`
	SCHEMA_VERSION string `json:"AVRO_SCHEMA_VERSION,omitempty"`
}
