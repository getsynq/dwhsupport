package service

type DatabaseCommand interface {
	isDatabaseCommand()
}
type FetchFullMetrics struct{}

func (r FetchFullMetrics) isDatabaseCommand() {}

type FetchFullCatalog struct{}

func (r FetchFullCatalog) isDatabaseCommand() {}
