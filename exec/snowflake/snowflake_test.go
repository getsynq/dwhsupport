package snowflake

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type SnowflakePrivateKeyTestSuite struct {
	suite.Suite
}

func TestSnowflakePrivateKeyTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakePrivateKeyTestSuite))
}

func (s *SnowflakePrivateKeyTestSuite) TestParseUnencryptedPKCS8PrivateKey() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	privKey, err := parsePrivateKey(keyBytes, "")
	s.Require().NoError(err)
	s.NotNil(privKey)
	s.NotNil(privKey.PublicKey)
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	privKey, err := parsePrivateKey(keyBytes, "testpassphrase")
	s.Require().NoError(err)
	s.NotNil(privKey)
	s.NotNil(privKey.PublicKey)
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithWrongPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "wrongpassphrase")
	s.Error(err)
	s.Contains(err.Error(), "failed to parse encrypted private key")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithoutPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "")
	s.Error(err)
	s.Contains(err.Error(), "encrypted private key is provided but no passphrase is set")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseInvalidPEMData() {
	_, err := parsePrivateKey([]byte("not a valid pem"), "")
	s.Error(err)
	s.Contains(err.Error(), "failed to decode PEM block")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEmptyPEMData() {
	_, err := parsePrivateKey([]byte(""), "")
	s.Error(err)
	s.Contains(err.Error(), "failed to decode PEM block")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseUnencryptedPKCS8PrivateKeyWithPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "somepassphrase")
	s.Error(err)
	s.Contains(err.Error(), "passphrase provided but private key is not encrypted")
}
