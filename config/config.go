package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/bufbuild/protovalidate-go"
	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yaml"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

func LoadConfig() (*agentdwhv1.Config, error) {
	// add driver for support yaml content

	configLoader := config.New("synq-dwh", config.ParseEnv, config.ParseTime, config.WithTagName("json"))
	configLoader.AddDriver(yaml.Driver)
	if err := configLoader.LoadExists("agent.yaml"); err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	agent := &agentdwhv1.Config_Agent{
		Name: hostname,
	}
	synq := &agentdwhv1.Config_SYNQ{
		ClientId:     "",
		ClientSecret: "",
		Endpoint:     "developer.synq.io:443",
		OauthUrl:     "https://developer.synq.io/oauth2/token",
	}
	connections := make(map[string]*agentdwhv1.Config_Connection)
	if s := configLoader.String("agent.name"); s != "" {
		agent.Name = s
	}
	if s := configLoader.String("agent.log_level"); s != "" {
		if v, ok := agentdwhv1.Config_Agent_LogLevel_value[strings.ToUpper(s)]; ok {
			agent.LogLevel = agentdwhv1.Config_Agent_LogLevel(v)
		} else if v, ok := agentdwhv1.Config_Agent_LogLevel_value[fmt.Sprintf("LOG_LEVEL_%s", strings.ToUpper(s))]; ok {
			agent.LogLevel = agentdwhv1.Config_Agent_LogLevel(v)
		}
	}
	agent.LogJson = lo.ToPtr(configLoader.Bool("agent.log_json", false))
	agent.LogReportCaller = lo.ToPtr(configLoader.Bool("agent.log_report_caller", false))

	if err := configLoader.BindStruct("synq", synq); err != nil {
		return nil, err
	}

	for connectionId := range configLoader.Sub("connections") {
		connection := &agentdwhv1.Config_Connection{}
		connections[connectionId] = connection
		sub := configLoader.Sub(fmt.Sprintf("connections.%s", connectionId))
		for key := range sub {
			switch key {
			case "parallelism":
				connection.Parallelism = int32(configLoader.Int(fmt.Sprintf("connections.%s.parallelism")))
			case "name":
				connection.Name = configLoader.String(fmt.Sprintf("connections.%s.name", connectionId))
			case "postgres":
				postgres := &agentdwhv1.PostgresConf{}
				if err := configLoader.BindStruct(fmt.Sprintf("connections.%s.postgres", connectionId), postgres); err != nil {
					return nil, err
				}
				connection.Config = &agentdwhv1.Config_Connection_Postgres{
					Postgres: postgres,
				}
			case "snowflake":
				snowflake := &agentdwhv1.SnowflakeConf{}
				if err := configLoader.BindStruct(fmt.Sprintf("connections.%s.snowflake", connectionId), snowflake); err != nil {
					return nil, err
				}
				connection.Config = &agentdwhv1.Config_Connection_Snowflake{
					Snowflake: snowflake,
				}
			case "bigquery":
				bigquery := &agentdwhv1.BigQueryConf{}
				if err := configLoader.BindStruct(fmt.Sprintf("connections.%s.bigquery", connectionId), bigquery); err != nil {
					return nil, err
				}
				connection.Config = &agentdwhv1.Config_Connection_Bigquery{
					Bigquery: bigquery,
				}
			case "redshift":
				redshift := &agentdwhv1.RedshiftConf{}
				if err := configLoader.BindStruct(fmt.Sprintf("connections.%s.redshift", connectionId), redshift); err != nil {
					return nil, err
				}
				connection.Config = &agentdwhv1.Config_Connection_Redshift{
					Redshift: redshift,
				}
			case "mysql":
				mysql := &agentdwhv1.MySQLConf{}
				if err := configLoader.BindStruct(fmt.Sprintf("connections.%s.mysql", connectionId), mysql); err != nil {
					return nil, err
				}
				connection.Config = &agentdwhv1.Config_Connection_Mysql{
					Mysql: mysql,
				}
			default:
				logrus.Warnf("Unknown key %s in connection %s", key, connectionId)
			}
		}

		if connection.Parallelism == 0 {
			connection.Parallelism = 2
		}
	}

	protoConf := &agentdwhv1.Config{
		Agent:       agent,
		Synq:        synq,
		Connections: connections,
	}

	validator, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	if err := validator.Validate(protoConf); err != nil {
		return nil, err
	}

	return protoConf, nil
}
