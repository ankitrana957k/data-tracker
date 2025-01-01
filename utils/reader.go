package utils

import (
	"encoding/json"
	"io"
	"os"

	"github.com/krogertechnology/data-tracker/models"
)

func ReadFromJSON(file io.Reader) ([]models.Config, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var kafkaConfigs []models.Config
	err = json.Unmarshal(data, &kafkaConfigs)

	return kafkaConfigs, err
}

func ProcessJSONFile(URI string) ([]models.Config, error) {
	file, err := os.Open(URI)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	configs, err := ReadFromJSON(file)
	if err != nil {
		return nil, err
	}

	return configs, nil
}
