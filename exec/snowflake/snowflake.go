package snowflake

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
)

// https://github.com/snowflakedb/gosnowflake/blob/099708d318689634a558f705ccc19b3b7b278972/doc.go#L107
const SPNApplicationId = "SYNQ_Platform"

type SnowflakeConf struct {
	User                 string
	Password             string
	PrivateKey           []byte
	PrivateKeyFile       string
	PrivateKeyPassphrase string
	Account              string
	Warehouse            string
	Databases            []string
	Role                 string
}

type Executor interface {
	stdsql.StdSqlExecutor
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &SnowflakeExecutor{}

type SnowflakeExecutor struct {
	conf *SnowflakeConf
	db   *sqlx.DB
}

func (e *SnowflakeExecutor) GetDb() *sqlx.DB {
	return e.db
}

// parsePrivateKey decodes and parses a PEM-encoded private key.
// It supports both unencrypted PKCS8 keys and encrypted PKCS8 keys with a passphrase.
func parsePrivateKey(privateKeyPEM []byte, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyPEM)
	if block == nil {
		return nil, errors.New("failed to decode PEM block containing private key")
	}

	var privKey interface{}
	var err error

	// Handle encrypted private keys
	if block.Type == "ENCRYPTED PRIVATE KEY" {
		if passphrase == "" {
			return nil, errors.New("encrypted private key is provided but no passphrase is set")
		} else {
			privKey, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse encrypted private key")
			}
		}
	} else if block.Type == "PRIVATE KEY" {
		privKey, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse private key")
		}
	} else {
		return nil, errors.Errorf("unsupported PEM block type: %s", block.Type)
	}

	rsaKey, ok := privKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not an RSA key")
	}

	return rsaKey, nil
}

func NewSnowflakeExecutor(ctx context.Context, conf *SnowflakeConf) (*SnowflakeExecutor, error) {

	database := ""
	if len(conf.Databases) > 0 {
		database = conf.Databases[0]
	}

	c := &gosnowflake.Config{
		Account:             conf.Account,
		User:                conf.User,
		Password:            conf.Password,
		Warehouse:           conf.Warehouse,
		Role:                conf.Role,
		Database:            database,
		Application:         SPNApplicationId,
		Params:              map[string]*string{},
		DisableConsoleLogin: gosnowflake.ConfigBoolTrue,
	}

	// Load private key from either inline bytes or file
	var privateKeyPEM []byte
	if len(conf.PrivateKey) > 0 {
		privateKeyPEM = conf.PrivateKey
	} else if len(conf.PrivateKeyFile) > 0 {
		keyData, err := os.ReadFile(conf.PrivateKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read private key file")
		}
		privateKeyPEM = keyData
	}

	if len(privateKeyPEM) > 0 {
		privKey, err := parsePrivateKey(privateKeyPEM, conf.PrivateKeyPassphrase)
		if err != nil {
			return nil, err
		}
		c.PrivateKey = privKey
		c.Authenticator = gosnowflake.AuthTypeJwt
	}

	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *c)
	stdDb := sql.OpenDB(connector)
	db := sqlx.NewDb(stdDb, "snowflake")

	err := db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &SnowflakeExecutor{conf: conf, db: db}, nil
}

func (e *SnowflakeExecutor) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, q, args...)
}

func (e *SnowflakeExecutor) Exec(ctx context.Context, sql string) error {
	_, err := e.db.ExecContext(ctx, sql)
	return err
}

func (e *SnowflakeExecutor) Close() error {
	return e.db.Close()
}

type Mock struct {
	SnowflakeExecutor
	queryRowsReturns func() (*sqlx.Rows, error)
}

func (bq *Mock) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	if bq.queryRowsReturns == nil {
		return nil, fmt.Errorf("no return defined in query rows mock")
	}
	return bq.queryRowsReturns()
}
