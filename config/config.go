package config

import (
	"os"

	"github.com/bufbuild/protovalidate-go"
	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yaml"
)

func LoadConfig() (*agentdwhv1.Config, error) {
	// add driver for support yaml content
	config.AddDriver(yaml.Driver)
	config.WithOptions(config.ParseEnv, config.ParseTime, config.WithTagName("json"))
	if err := config.LoadExists("agent.yaml"); err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	protoConf := &agentdwhv1.Config{
		Agent: &agentdwhv1.Config_Agent{
			Name: hostname,
		},
		Synq: &agentdwhv1.Config_SYNQ{
			ClientId:     "",
			ClientSecret: "",
			Endpoint:     "developer.synq.io:443",
			OauthUrl:     "https://developer.synq.io/oauth2/token",
		},
	}
	if err := config.BindStruct("", protoConf); err != nil {
		return nil, err
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
