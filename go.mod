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
	github.com/pkg/errors v0.9.1
	github.com/samber/lo v1.49.1
	github.com/sirupsen/logrus v1.9.3
	golang.org/x/oauth2 v0.25.0
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.36.4
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.4-20250130201111-63bb56e20495.1 // indirect
	cel.dev/expr v0.19.2 // indirect
	cloud.google.com/go v0.118.1 // indirect
	cloud.google.com/go/auth v0.14.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.7 // indirect
	cloud.google.com/go/bigquery v1.66.2 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.3.1 // indirect
	dario.cat/mergo v1.0.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.9.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.2.0 // indirect
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/ClickHouse/ch-go v0.64.1 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.30.3 // indirect
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/apache/arrow/go/v12 v12.0.1 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/apache/arrow/go/v16 v16.0.0 // indirect
	github.com/apache/thrift v0.21.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.26.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.11 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.16.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.3.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.17.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.53.1 // indirect
	github.com/aws/smithy-go v1.20.2 // indirect
	github.com/coreos/go-oidc/v3 v3.5.0 // indirect
	github.com/danieljoos/wincred v1.2.0 // indirect
	github.com/databricks/databricks-sdk-go v0.57.0 // indirect
	github.com/databricks/databricks-sql-go v1.6.1 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/dvsekhvalnov/jose2go v1.6.0 // indirect
	github.com/envoyproxy/go-control-plane v0.13.1 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/gabriel-vasile/mimetype v1.4.7 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-jose/go-jose/v3 v3.0.3 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/goccy/go-yaml v1.11.2 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/cel-go v0.23.0 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/gookit/color v1.5.4 // indirect
	github.com/gookit/goutil v0.6.15 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.7 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20231216201459-8508981c8b6c // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/snowflakedb/gosnowflake v1.13.0 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/xxjwxc/gowp v0.0.0-20240929033016-5be68d222389 // indirect
	github.com/xxjwxc/public v0.0.0-20210518123934-6cc0965f0bc5 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.58.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0 // indirect
	go.opentelemetry.io/otel v1.34.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	go.opentelemetry.io/otel/trace v1.34.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/term v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.9.0 // indirect
	golang.org/x/tools v0.29.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.220.0 // indirect
	google.golang.org/genproto v0.0.0-20250122153221-138b5a5a4fd4 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250127172529-29210b9bc287 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250127172529-29210b9bc287 // indirect
	gopkg.in/eapache/queue.v1 v1.1.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gotest.tools/gotestsum v1.8.2 // indirect
)
