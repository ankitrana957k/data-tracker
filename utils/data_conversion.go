package utils

import (
	"encoding/json"
	"strings"
)

func FormatJSONString(s []byte) ([]byte, error) {
	var jsonData map[string]interface{}

	if err := json.Unmarshal(s, &jsonData); err != nil {
		return nil, err
	}

	prettyJSON, err := json.MarshalIndent(jsonData, "", "\t")
	if err != nil {
		return nil, err
	}

	return prettyJSON, nil
}

func IsJSON(data []byte) bool {
	var js map[string]interface{}
	return json.Unmarshal(data, &js) == nil
}

func GetElementsFromString(s string) []string {
	elements := strings.Split(s, ",")

	for i, broker := range elements {
		elements[i] = strings.TrimSpace(broker)
	}

	return elements
}
