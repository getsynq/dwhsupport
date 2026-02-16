#!/bin/bash

# Change to the directory where this script is located
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

echo "Generating mocks for dwhsupport"

go tool go.uber.org/mock/mockgen -typed -package=scrapper -destination=scrapper/mocks.go github.com/getsynq/dwhsupport/scrapper Scrapper
go tool go.uber.org/mock/mockgen -typed -package=querier -destination=exec/querier/mocks.go github.com/getsynq/dwhsupport/exec/querier Querier

echo "Mock generation complete"
