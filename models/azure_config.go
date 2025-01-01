package models

type AzureConfig struct {
	AAD_AUDIENCE           string `json:"AAD_AUDIENCE,omitempty"`
	AAD_APPLICATION_ID     string `json:"AAD_APPLICATION_ID,omitempty"`
	AAD_APPLICATION_SECRET string `json:"AAD_APPLICATION_SECRET,omitempty"`
	AAD_TENANT_ID          string `json:"AAD_TENANT_ID,omitempty"`
}
