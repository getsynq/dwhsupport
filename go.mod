module github.com/getsynq/synq-dwh

go 1.23.3

replace github.com/getsynq/api => ./gen_public/github.com/getsynq/api

replace github.com/getsynq/dwhsupport => ./dwhsupport

require (
	github.com/bufbuild/protovalidate-go v0.9.1
	github.com/getsynq/api v0.0.0-00010101000000-000000000000
	github.com/getsynq/dwhsupport v0.0.0-20250204133829-646166a1a132
	github.com/google/uuid v1.6.0
	github.com/gookit/config/v2 v2.2.5
	github.com/sirupsen/logrus v1.9.3
	golang.org/x/oauth2 v0.25.0
	google.golang.org/grpc v1.70.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.4-20250130201111-63bb56e20495.1 // indirect
	cel.dev/expr v0.19.1 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	dario.cat/mergo v1.0.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/goccy/go-yaml v1.11.2 // indirect
	github.com/google/cel-go v0.23.0 // indirect
	github.com/gookit/color v1.5.4 // indirect
	github.com/gookit/goutil v0.6.15 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20231216201459-8508981c8b6c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/samber/lo v1.49.1 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/term v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241209162323-e6fa225c2576 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250106144421-5f5ef82da422 // indirect
	google.golang.org/protobuf v1.36.4 // indirect
)
