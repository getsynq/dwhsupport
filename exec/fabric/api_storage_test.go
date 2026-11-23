package fabric

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type APIStorageSuite struct {
	suite.Suite
}

func TestAPIStorageSuite(t *testing.T) {
	suite.Run(t, new(APIStorageSuite))
}

func (s *APIStorageSuite) fixture(name string) []byte {
	body, err := os.ReadFile("testdata/" + name)
	s.Require().NoError(err)
	return body
}

// server answers each path with a handler, and records the requests.
func (s *APIStorageSuite) server(handlers map[string]http.HandlerFunc) (*httptest.Server, *[]string) {
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		handler, ok := handlers[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"errorCode":"EntityNotFound","message":"no such path"}`))
			return
		}
		handler(w, r)
	}))
	s.T().Cleanup(srv.Close)
	return srv, &requests
}

func answer(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

func (s *APIStorageSuite) TestListStorageItemsReadsEachTypeFromItsListing() {
	ws := "/workspaces/ws-1/"
	srv, requests := s.server(map[string]http.HandlerFunc{
		ws + "lakehouses":        answer(s.fixture("lakehouses.json")),
		ws + "warehouses":        answer(s.fixture("warehouses.json")),
		ws + "mirroredDatabases": answer(s.fixture("mirroredDatabases.json")),
		ws + "sqlDatabases":      answer(s.fixture("sqlDatabases.json")),
	})

	items, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListStorageItems(context.Background(), "ws-1")
	s.Require().NoError(err)
	s.Equal([]string{
		"GET /workspaces/ws-1/lakehouses",
		"GET /workspaces/ws-1/warehouses",
		"GET /workspaces/ws-1/mirroredDatabases",
		"GET /workspaces/ws-1/sqlDatabases",
	}, *requests)

	host := "lvmghcalzphu5hhdgghdffivni-6apfgq3vf4eexhtn4fl5xrwg7i.datawarehouse.fabric.microsoft.com"
	workspace := "43531ef0-2f75-4b08-9e6d-e157dbc6c6fa"
	s.Equal([]*StorageItem{
		{
			ID: "8867ec2b-64bb-4d3d-bc08-57a147aa3350", WorkspaceID: workspace, Type: ItemTypeLakehouse, DisplayName: "COALESCE_QUALITY_LH",
			SQLEndpointHost: host, SQLEndpointID: "cb9726b9-0359-4c38-aa12-3070089db623", SQLEndpointStatus: "Success",
		},
		{
			ID: "404f2e37-9d18-4590-bbf6-56ff987b3e05", WorkspaceID: workspace, Type: ItemTypeWarehouse, DisplayName: "COALESCE_QUALITY_DWHTESTING",
			SQLEndpointHost: host,
		},
		{
			ID: "998219a9-daf9-4ddd-afee-c0ce45804d33", WorkspaceID: workspace, Type: ItemTypeMirroredDatabase, DisplayName: "COALESCE_QUALITY_MIRROR",
			SQLEndpointHost: host, SQLEndpointID: "06ae26f7-19e7-42aa-be95-0e856be61f90", SQLEndpointStatus: "Success",
		},
		{
			// The listing of a SQL database gives no SQL analytics endpoint.
			ID: "1bea3694-2705-471b-90bb-007e81797312", WorkspaceID: workspace, Type: ItemTypeSQLDatabase, DisplayName: "COALESCE_QUALITY_SQLDB",
		},
	}, items)
}

func (s *APIStorageSuite) TestListStorageItemsFollowsPagesAndFillsAMissingWorkspace() {
	ws := "/workspaces/ws-1/"
	empty := answer([]byte(`{"value":[]}`))
	srv, requests := s.server(map[string]http.HandlerFunc{
		ws + "lakehouses": func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("continuationToken") == "" {
				_, _ = w.Write([]byte(`{"value":[{"id":"lh-1","displayName":"A","properties":{"defaultSchema":"dbo"}}],"continuationToken":"page 2"}`))
				return
			}
			_, _ = w.Write([]byte(`{"value":[{"id":"lh-2","displayName":"B"}]}`))
		},
		ws + "warehouses":        empty,
		ws + "mirroredDatabases": empty,
		ws + "sqlDatabases":      empty,
	})

	items, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListStorageItems(context.Background(), "ws-1")
	s.Require().NoError(err)
	s.Contains(*requests, "GET /workspaces/ws-1/lakehouses?continuationToken=page+2")
	s.Require().Len(items, 2)
	s.Equal("ws-1", items[0].WorkspaceID)
	s.Equal("dbo", items[0].DefaultSchema)
	s.True(items[0].HasSchemas(), "a Lakehouse with a default schema has schemas")
	s.False(items[1].HasSchemas(), "a Lakehouse with no default schema keeps its tables in dbo")
}

func (s *APIStorageSuite) TestListStorageItemsFailsOnAFailedListing() {
	ws := "/workspaces/ws-1/"
	srv, _ := s.server(map[string]http.HandlerFunc{
		ws + "lakehouses": answer([]byte(`{"value":[{"id":"lh-1","displayName":"A"}]}`)),
		ws + "warehouses": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errorCode":"InsufficientWorkspaceRole","message":"no"}`))
		},
	})

	items, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListStorageItems(context.Background(), "ws-1")
	s.Require().Error(err)
	s.Nil(items, "a part of the listing is never returned")
	s.True(IsPermissionError(err))
}

func (s *APIStorageSuite) TestHasSchemas() {
	s.True((&StorageItem{Type: ItemTypeWarehouse}).HasSchemas())
	s.True((&StorageItem{Type: ItemTypeMirroredDatabase}).HasSchemas())
	s.True((&StorageItem{Type: ItemTypeSQLDatabase}).HasSchemas())
	s.False((&StorageItem{Type: ItemTypeLakehouse}).HasSchemas())
	s.True((&StorageItem{Type: ItemTypeLakehouse, DefaultSchema: "dbo"}).HasSchemas())
}

func (s *APIStorageSuite) TestListShortcutsTrimsThePaths() {
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/items/lh-1/shortcuts": func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("continuationToken") == "" {
				var page map[string]any
				s.Require().NoError(json.Unmarshal(s.fixture("shortcuts.json"), &page))
				page["continuationToken"] = "next"
				body, _ := json.Marshal(page)
				_, _ = w.Write(body)
				return
			}
			_, _ = w.Write([]byte(`{"value":[{"name":"landing","path":"Files/raw/","target":{"type":"AmazonS3","amazonS3":{"location":"https://bucket.s3.amazonaws.com"}}}]}`))
		},
	})

	shortcuts, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListShortcuts(context.Background(), "ws-1", "lh-1")
	s.Require().NoError(err)
	s.Equal([]*Shortcut{
		{
			Name: "wh_sales_orders", Path: "Tables", TargetType: "OneLake",
			OneLake: &OneLakeTarget{
				WorkspaceID: "43531ef0-2f75-4b08-9e6d-e157dbc6c6fa",
				ItemID:      "404f2e37-9d18-4590-bbf6-56ff987b3e05",
				Path:        "Tables/sales/orders",
			},
		},
		{Name: "landing", Path: "Files/raw", TargetType: "AmazonS3"},
	}, shortcuts)
}

func definitionAnswer(mirroring []byte) []byte {
	parts := []map[string]string{
		{"path": ".platform", "payload": base64.StdEncoding.EncodeToString([]byte(`{}`)), "payloadType": "InlineBase64"},
	}
	if mirroring != nil {
		parts = append(parts, map[string]string{
			"path": "mirroring.json", "payload": base64.StdEncoding.EncodeToString(mirroring), "payloadType": "InlineBase64",
		})
	}
	body, _ := json.Marshal(map[string]any{"definition": map[string]any{"parts": parts}})
	return body
}

func (s *APIStorageSuite) TestGetMirroringDefinitionReadsTheSource() {
	srv, requests := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/mirroredDatabases/md-1/getDefinition": func(w http.ResponseWriter, r *http.Request) {
			s.Equal(http.MethodPost, r.Method)
			s.Equal("0", r.Header.Get("Content-Length"), "Fabric answers 411 to a POST with no length")
			_, _ = w.Write(definitionAnswer(s.fixture("mirroring_snowflake.json")))
		},
	})

	definition, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).GetMirroringDefinition(context.Background(), "ws-1", "md-1")
	s.Require().NoError(err)
	s.Equal([]string{"POST /workspaces/ws-1/mirroredDatabases/md-1/getDefinition"}, *requests)
	s.Equal(&MirroringDefinition{
		SourceType:   "Snowflake",
		ConnectionID: "6b2b3e1c-6f0a-4c39-9d5d-2a7c1b0e9f11",
		Database:     "DWH_TESTING",
		// The mounted table with no table name is left out.
		Tables: []MirroredTable{{Schema: "SCHEMA_B", Table: "CUSTOMERS"}, {Schema: "PUBLIC", Table: "ORDERS"}},
	}, definition)
}

func (s *APIStorageSuite) TestGetMirroringDefinitionWaitsForTheOperation() {
	var polls atomic.Int32
	var srvURL string
	srv, requests := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/mirroredDatabases/md-1/getDefinition": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", srvURL+"/operations/op-1")
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusAccepted)
		},
		"/operations/op-1": func(w http.ResponseWriter, _ *http.Request) {
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"status":"Running"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"Succeeded"}`))
		},
		"/operations/op-1/result": answer(s.fixture("mirror_definition_generic.json")),
	})
	srvURL = srv.URL

	definition, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).GetMirroringDefinition(context.Background(), "ws-1", "md-1")
	s.Require().NoError(err)
	s.Equal([]string{
		"POST /workspaces/ws-1/mirroredDatabases/md-1/getDefinition",
		"GET /operations/op-1",
		"GET /operations/op-1",
		"GET /operations/op-1/result",
	}, *requests)
	s.Equal(&MirroringDefinition{SourceType: "GenericMirror"}, definition, "open mirroring names no source")
}

func (s *APIStorageSuite) TestGetMirroringDefinitionFailedOperation() {
	var srvURL string
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/mirroredDatabases/md-1/getDefinition": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", srvURL+"/operations/op-1")
			w.WriteHeader(http.StatusAccepted)
		},
		"/operations/op-1": answer([]byte(`{"status":"Failed","error":{"errorCode":"ItemDefinitionNotSupported","message":"no"}}`)),
	})
	srvURL = srv.URL

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).GetMirroringDefinition(context.Background(), "ws-1", "md-1")
	var apiErr *APIError
	s.Require().True(errors.As(err, &apiErr))
	s.Equal("ItemDefinitionNotSupported", apiErr.ErrorCode)
	s.False(IsPermissionError(err))
}

func (s *APIStorageSuite) TestGetMirroringDefinitionWithoutTheMirroringPart() {
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/mirroredDatabases/md-1/getDefinition": answer(definitionAnswer(nil)),
	})

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).GetMirroringDefinition(context.Background(), "ws-1", "md-1")
	s.ErrorIs(err, ErrDefinitionWithoutMirroring)
}

func (s *APIStorageSuite) TestGetMirroringDefinitionRefusedForAViewer() {
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/mirroredDatabases/md-1/getDefinition": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errorCode":"InsufficientWorkspaceRole","message":"no"}`))
		},
	})

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).GetMirroringDefinition(context.Background(), "ws-1", "md-1")
	s.True(IsPermissionError(err))
}

func (s *APIStorageSuite) TestThrottledRequestIsRetried() {
	var calls atomic.Int32
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/items/md-1/connections": func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{"value":[{"id":"c-1","displayName":"Snowflake prod",
				"connectionDetails":{"type":"Snowflake","path":"acme-prod.snowflakecomputing.com;COMPUTE_WH"}},{"id":"c-2","displayName":"No details"}]}`))
		},
	})

	connections, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListItemConnections(context.Background(), "ws-1", "md-1")
	s.Require().NoError(err)
	s.Equal(int32(2), calls.Load())
	s.Equal([]*ItemConnection{
		{ID: "c-1", DisplayName: "Snowflake prod", Type: "Snowflake", Path: "acme-prod.snowflakecomputing.com;COMPUTE_WH"},
		{ID: "c-2", DisplayName: "No details"},
	}, connections)
}

func (s *APIStorageSuite) TestThrottledRequestFailsAfterTheRetries() {
	var calls atomic.Int32
	srv, _ := s.server(map[string]http.HandlerFunc{
		"/workspaces/ws-1/items/md-1/connections": func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		},
	})

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListItemConnections(context.Background(), "ws-1", "md-1")
	var apiErr *APIError
	s.Require().True(errors.As(err, &apiErr))
	s.Equal(http.StatusTooManyRequests, apiErr.StatusCode)
	s.Equal(int32(maxThrottledRetries+1), calls.Load())
	s.False(IsPermissionError(err), "a throttled request is no refusal")
}

func (s *APIStorageSuite) TestIsNotFound() {
	s.True(IsNotFound(errors.WithStack(&APIError{StatusCode: http.StatusNotFound})))
	s.True(IsNotFound(&APIError{StatusCode: http.StatusBadRequest, ErrorCode: "WorkspaceNotFound"}))
	s.False(IsNotFound(&APIError{StatusCode: http.StatusBadRequest, ErrorCode: "InvalidInput"}))
	s.False(IsNotFound(&APIError{StatusCode: http.StatusForbidden}))
	s.False(IsNotFound(errors.New("network")))
}

func TestParseMirroringDefinition(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    *MirroringDefinition
	}{
		{
			name:    "every table of the source",
			payload: `{"properties":{"source":{"type":"AzureSqlDatabase","typeProperties":{"connection":"c-1","database":"sales"}}}}`,
			want:    &MirroringDefinition{SourceType: "AzureSqlDatabase", ConnectionID: "c-1", Database: "sales"},
		},
		{
			name:    "a mounted table with no schema",
			payload: `{"properties":{"source":{"type":"Snowflake"},"mountedTables":[{"source":{"typeProperties":{"tableName":"T"}}}]}}`,
			want:    &MirroringDefinition{SourceType: "Snowflake"},
		},
		{
			name:    "empty",
			payload: `{}`,
			want:    &MirroringDefinition{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseMirroringDefinition([]byte(tc.payload))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
	_, err := ParseMirroringDefinition([]byte(`not json`))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "mirroring.json"))
}
