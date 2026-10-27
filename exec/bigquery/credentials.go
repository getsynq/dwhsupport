package bigquery

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

// GoogleTokenURI is the token endpoint every Google service account key names.
const GoogleTokenURI = "https://oauth2.googleapis.com/token"

// ErrNoCredential is returned for a connection that names no credential and does not allow the
// process's default credentials.
var ErrNoCredential = errors.New("bigquery: no credential configured, set a service account key or an access token")

type credentialFile struct {
	Type        string `json:"type"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

// ValidateServiceAccountKey accepts only a Google service account key that gets its tokens from
// Google. Every other credential type makes the process read a file or call a URL the credential
// names (external_account reads its subject token from one), and a key with another token_uri posts
// to that address. A credential that someone other than the host's owner supplies must pass this.
func ValidateServiceAccountKey(key []byte) error {
	var f credentialFile
	if err := json.Unmarshal(key, &f); err != nil {
		return errors.New("bigquery: the service account key is not valid JSON")
	}
	if f.Type != string(option.ServiceAccount) {
		return fmt.Errorf("bigquery: the credential must be a service account key (type %q), got type %q", option.ServiceAccount, f.Type)
	}
	if f.ClientEmail == "" || f.PrivateKey == "" {
		return errors.New("bigquery: the service account key has no client_email or private_key")
	}
	if f.TokenURI != "" && f.TokenURI != GoogleTokenURI {
		return fmt.Errorf("bigquery: the service account key must use the token endpoint %s", GoogleTokenURI)
	}
	return nil
}

// credentialOption authenticates with the credential conf names. AccessToken is handled by the
// caller, as it is not a credential file.
func credentialOption(conf *BigQueryConf) (option.ClientOption, error) {
	var key []byte
	switch {
	case conf.CredentialsJson != "":
		key = []byte(conf.CredentialsJson)
	case conf.CredentialsFile != "":
		content, err := os.ReadFile(conf.CredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "bigquery: reading the credentials file")
		}
		key = content
	case conf.AllowDefaultCredentials:
		return nil, nil
	default:
		return nil, ErrNoCredential
	}

	if !conf.AllowAnyCredentialType {
		if err := ValidateServiceAccountKey(key); err != nil {
			return nil, err
		}
		return option.WithAuthCredentialsJSON(option.ServiceAccount, key), nil
	}

	var f credentialFile
	if err := json.Unmarshal(key, &f); err != nil {
		return nil, errors.New("bigquery: the credential is not valid JSON")
	}
	if f.Type == "" {
		return nil, errors.New("bigquery: the credential has no type")
	}
	return option.WithAuthCredentialsJSON(option.CredentialsType(f.Type), key), nil
}
