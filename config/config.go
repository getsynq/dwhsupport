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
	if err := configLoader.BindStruct("connections", &connections); err != nil {
		return nil, err
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
