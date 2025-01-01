package kafka

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/krogertechnology/data-tracker/models"
	"github.com/linkedin/goavro"
)

type AvroConfig models.AvroConfig

func (a *AvroConfig) ProcessAvroMessage(msg models.Message) (*models.Message, error) {
	if a == nil || a.SCHEMA_URL == "" {
		return nil, errors.New("no scema found for the data")
	}

	value := msg.Value
	schemaID := binary.BigEndian.Uint32(value[1:5])

	finalMsg := models.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     msg.Value[5:],
		Headers:   msg.Headers,
	}

	resp, err := http.Get(fmt.Sprintf("%s/schemas/ids/%d", a.SCHEMA_URL, schemaID))
	if err != nil {
		return nil, err
	}

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	schemaResp := struct {
		SchemaID string
		Schema   string
	}{}

	err = json.Unmarshal(respData, &schemaResp)
	if err != nil {
		return nil, err
	}

	schema := make(map[string]interface{}, 0)
	err = json.Unmarshal([]byte(schemaResp.Schema), &schema)
	if err != nil {
		return nil, err
	}

	schemaBytes, _ := json.Marshal(schema)

	codec, err := goavro.NewCodec(string(schemaBytes))
	if err != nil {
		log.Fatalf("Error creating Avro codec: %v", err)
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(msg.Value[5:])
	if err != nil {
		log.Fatalf("Error decoding Avro data: %v", err)
		return nil, err
	}

	finalMsg.Value, _ = json.Marshal(native)

	return &finalMsg, err
}
