package kafka

import (
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/IBM/sarama"
)

type TokenProvider struct {
	servicePrincipalToken *azidentity.ClientSecretCredential
	audiences             []string
}

func NewTokenProvider(k *KafkaConfig) sarama.AccessTokenProvider {
	audience := strings.Split(k.AAD_AUDIENCE, ",")

	for i, aud := range audience {
		if !strings.HasSuffix(aud, "//.default") {
			audience[i] = aud + "//.default"
		}
	}

	clientOpts := azcore.ClientOptions{Cloud: cloud.AzurePublic}

	credential, err := azidentity.NewClientSecretCredential(k.AAD_TENANT_ID, k.AAD_APPLICATION_ID, k.AAD_APPLICATION_SECRET,
		&azidentity.ClientSecretCredentialOptions{ClientOptions: clientOpts})
	if err != nil {
		return nil
	}

	return &TokenProvider{
		audiences:             audience,
		servicePrincipalToken: credential,
	}
}

// Token returns a new *sarama.AccessToken or an error as appropriate.
func (t *TokenProvider) Token() (*sarama.AccessToken, error) {
	accessToken, _ := t.servicePrincipalToken.GetToken(context.TODO(), policy.TokenRequestOptions{Scopes: t.audiences})

	return &sarama.AccessToken{Token: accessToken.Token}, nil
}
