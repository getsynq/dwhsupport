package fabric

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

// The Fabric item types that the SQL analytics endpoint shows as a database.
const (
	ItemTypeLakehouse        = "Lakehouse"
	ItemTypeWarehouse        = "Warehouse"
	ItemTypeMirroredDatabase = "MirroredDatabase"
	ItemTypeSQLDatabase      = "SQLDatabase"
)

// storageItemListings maps each storage item type to the path of its typed
// listing. The typed listings give the properties that the generic item listing
// does not: the SQL analytics endpoint and the default schema.
var storageItemListings = []struct {
	itemType string
	path     string
}{
	{ItemTypeLakehouse, "lakehouses"},
	{ItemTypeWarehouse, "warehouses"},
	{ItemTypeMirroredDatabase, "mirroredDatabases"},
	{ItemTypeSQLDatabase, "sqlDatabases"},
}

// StorageItem is a Fabric item that the SQL analytics endpoint shows as a
// database: a Lakehouse, a Warehouse, a mirrored database or a SQL database. Its
// display name is the name of that database.
type StorageItem struct {
	ID          string
	WorkspaceID string
	// Type is one of the ItemType constants.
	Type        string
	DisplayName string
	Description string
	// SQLEndpointHost is the host of the SQL analytics endpoint of the item. It is
	// empty for a SQL database, whose listing does not give it.
	SQLEndpointHost string
	// SQLEndpointID is the GUID of the SQL analytics endpoint of a Lakehouse or a
	// mirrored database. A Warehouse is its own endpoint, so it is empty there.
	SQLEndpointID string
	// SQLEndpointStatus is the provisioning status of the SQL analytics endpoint of
	// a Lakehouse or a mirrored database, such as "Success", "InProgress" or
	// "Failed". It is empty for the other types.
	SQLEndpointStatus string
	// DefaultSchema is the default schema of a Lakehouse with schemas. It is empty
	// for a Lakehouse with no schemas, which keeps every table in dbo.
	DefaultSchema string
}

// HasSchemas is true if the tables of the item are below a schema in OneLake
// (Tables/<schema>/<table>): a Warehouse, a mirrored database, and a Lakehouse
// with schemas. A Lakehouse with no schemas keeps its tables at Tables/<table>.
func (i *StorageItem) HasSchemas() bool {
	return i.Type != ItemTypeLakehouse || i.DefaultSchema != ""
}

type storageItemsPage struct {
	Value []struct {
		ID          string `json:"id"`
		WorkspaceID string `json:"workspaceId"`
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
		Properties  struct {
			// A Warehouse gives its endpoint host as connectionString.
			ConnectionString      string `json:"connectionString"`
			DefaultSchema         string `json:"defaultSchema"`
			SQLEndpointProperties *struct {
				ConnectionString   string `json:"connectionString"`
				ID                 string `json:"id"`
				ProvisioningStatus string `json:"provisioningStatus"`
			} `json:"sqlEndpointProperties"`
		} `json:"properties"`
	} `json:"value"`
	ContinuationToken string `json:"continuationToken"`
}

// ListStorageItems lists the Lakehouses, Warehouses, mirrored databases and SQL
// databases of a workspace. It needs the Viewer role in the workspace. It returns
// an error if any page fails, never a part of the listing.
func (c *APIClient) ListStorageItems(ctx context.Context, workspaceID string) ([]*StorageItem, error) {
	var all []*StorageItem
	for _, listing := range storageItemListings {
		base := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/" + listing.path
		err := c.forEachPage(ctx, base, func(body json.RawMessage) (string, error) {
			var page storageItemsPage
			if err := json.Unmarshal(body, &page); err != nil {
				return "", errors.Wrapf(err, "decoding the %s of workspace %s", listing.path, workspaceID)
			}
			for _, v := range page.Value {
				item := &StorageItem{
					ID:          v.ID,
					WorkspaceID: v.WorkspaceID,
					Type:        listing.itemType,
					DisplayName: v.DisplayName,
					Description: v.Description,
				}
				if item.WorkspaceID == "" {
					item.WorkspaceID = workspaceID
				}
				switch listing.itemType {
				case ItemTypeWarehouse:
					item.SQLEndpointHost = v.Properties.ConnectionString
				case ItemTypeLakehouse, ItemTypeMirroredDatabase:
					item.DefaultSchema = v.Properties.DefaultSchema
					if endpoint := v.Properties.SQLEndpointProperties; endpoint != nil {
						item.SQLEndpointHost = endpoint.ConnectionString
						item.SQLEndpointID = endpoint.ID
						item.SQLEndpointStatus = endpoint.ProvisioningStatus
					}
				}
				all = append(all, item)
			}
			return page.ContinuationToken, nil
		})
		if err != nil {
			return nil, err
		}
	}
	return all, nil
}

// Shortcut is a OneLake shortcut of a Lakehouse.
type Shortcut struct {
	// Name is the name of the shortcut, the last segment of its OneLake path.
	Name string
	// Path is the folder that holds the shortcut, with no leading or trailing
	// slash: "Tables", "Tables/<schema>" or a folder below "Files".
	Path string
	// TargetType is the kind of target, such as "OneLake", "AdlsGen2" or
	// "AmazonS3".
	TargetType string
	// OneLake is the target of a shortcut to OneLake. Nil for the other targets.
	OneLake *OneLakeTarget
}

// OneLakeTarget is the item and the folder that a OneLake shortcut points to.
type OneLakeTarget struct {
	WorkspaceID string
	ItemID      string
	// Path is the folder in the target item, with no leading or trailing slash,
	// such as "Tables/sales/orders".
	Path string
}

type shortcutsPage struct {
	Value []struct {
		Name   string `json:"name"`
		Path   string `json:"path"`
		Target struct {
			Type    string `json:"type"`
			OneLake *struct {
				WorkspaceID string `json:"workspaceId"`
				ItemID      string `json:"itemId"`
				Path        string `json:"path"`
			} `json:"oneLake"`
		} `json:"target"`
	} `json:"value"`
	ContinuationToken string `json:"continuationToken"`
}

// ListShortcuts lists the OneLake shortcuts of an item. Fabric gives the path of
// a shortcut with a leading slash in the listing and without it in a single read,
// so the paths here have neither.
func (c *APIClient) ListShortcuts(ctx context.Context, workspaceID, itemID string) ([]*Shortcut, error) {
	var all []*Shortcut
	base := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/items/" + url.PathEscape(itemID) + "/shortcuts"
	err := c.forEachPage(ctx, base, func(body json.RawMessage) (string, error) {
		var page shortcutsPage
		if err := json.Unmarshal(body, &page); err != nil {
			return "", errors.Wrapf(err, "decoding the shortcuts of item %s", itemID)
		}
		for _, v := range page.Value {
			shortcut := &Shortcut{Name: v.Name, Path: strings.Trim(v.Path, "/"), TargetType: v.Target.Type}
			if target := v.Target.OneLake; target != nil {
				shortcut.OneLake = &OneLakeTarget{
					WorkspaceID: target.WorkspaceID,
					ItemID:      target.ItemID,
					Path:        strings.Trim(target.Path, "/"),
				}
			}
			all = append(all, shortcut)
		}
		return page.ContinuationToken, nil
	})
	if err != nil {
		return nil, err
	}
	return all, nil
}

// MirroringDefinition is what the mirroring.json part of the definition of a
// mirrored database says about its source.
type MirroringDefinition struct {
	// SourceType is `properties.source.type`, such as "Snowflake",
	// "AzureSqlDatabase" or "GenericMirror" (open mirroring).
	SourceType string
	// ConnectionID is the GUID of the Fabric connection to the source
	// (`properties.source.typeProperties.connection`). Empty for open mirroring.
	ConnectionID string
	// Database is the source database (`properties.source.typeProperties.database`).
	Database string
	// Tables are the source tables of `properties.mountedTables`. It is empty if
	// the mirrored database mirrors every table of the source, as the definition
	// then names none.
	Tables []MirroredTable
}

// MirroredTable is one source table of a mirrored database.
type MirroredTable struct {
	Schema string
	Table  string
}

type mirroringJSON struct {
	Properties struct {
		Source struct {
			Type           string `json:"type"`
			TypeProperties struct {
				Connection string `json:"connection"`
				Database   string `json:"database"`
			} `json:"typeProperties"`
		} `json:"source"`
		MountedTables []struct {
			Source struct {
				TypeProperties struct {
					SchemaName string `json:"schemaName"`
					TableName  string `json:"tableName"`
				} `json:"typeProperties"`
			} `json:"source"`
		} `json:"mountedTables"`
	} `json:"properties"`
}

// ErrDefinitionWithoutMirroring is the error of a mirrored database definition
// with no mirroring.json part.
var ErrDefinitionWithoutMirroring = errors.New("the definition of the mirrored database has no mirroring.json part")

type definitionResponse struct {
	Definition struct {
		Parts []struct {
			Path        string `json:"path"`
			Payload     string `json:"payload"`
			PayloadType string `json:"payloadType"`
		} `json:"parts"`
	} `json:"definition"`
}

// GetMirroringDefinition reads the definition of a mirrored database. It needs the
// Contributor role in the workspace, and Fabric refuses it for a Viewer with 403.
// Fabric answers with a long running operation, which the call waits for.
func (c *APIClient) GetMirroringDefinition(ctx context.Context, workspaceID, itemID string) (*MirroringDefinition, error) {
	u := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/mirroredDatabases/" + url.PathEscape(itemID) + "/getDefinition"
	var response definitionResponse
	if err := c.do(ctx, http.MethodPost, u, &response); err != nil {
		return nil, err
	}
	for _, part := range response.Definition.Parts {
		if part.Path != "mirroring.json" {
			continue
		}
		payload, err := base64.StdEncoding.DecodeString(part.Payload)
		if err != nil {
			return nil, errors.Wrapf(err, "decoding the mirroring.json of item %s", itemID)
		}
		return ParseMirroringDefinition(payload)
	}
	return nil, errors.WithStack(ErrDefinitionWithoutMirroring)
}

// ParseMirroringDefinition reads the mirroring.json part of a mirrored database
// definition. A mounted table with no schema or no table name is left out.
func ParseMirroringDefinition(payload []byte) (*MirroringDefinition, error) {
	var parsed mirroringJSON
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, errors.Wrap(err, "decoding mirroring.json")
	}
	source := parsed.Properties.Source
	definition := &MirroringDefinition{
		SourceType:   source.Type,
		ConnectionID: source.TypeProperties.Connection,
		Database:     source.TypeProperties.Database,
	}
	for _, mounted := range parsed.Properties.MountedTables {
		table := MirroredTable{Schema: mounted.Source.TypeProperties.SchemaName, Table: mounted.Source.TypeProperties.TableName}
		if table.Schema == "" || table.Table == "" {
			continue
		}
		definition.Tables = append(definition.Tables, table)
	}
	return definition, nil
}

// ItemConnection is a Fabric connection that an item uses.
type ItemConnection struct {
	ID          string
	DisplayName string
	// Type is `connectionDetails.type`, such as "Snowflake".
	Type string
	// Path is `connectionDetails.path`. For Snowflake it is
	// "<account>.snowflakecomputing.com;<warehouse>".
	Path string
}

type itemConnectionsPage struct {
	Value []struct {
		ID                string `json:"id"`
		DisplayName       string `json:"displayName"`
		ConnectionDetails *struct {
			Type string `json:"type"`
			Path string `json:"path"`
		} `json:"connectionDetails"`
	} `json:"value"`
	ContinuationToken string `json:"continuationToken"`
}

// ListItemConnections lists the Fabric connections that an item uses.
func (c *APIClient) ListItemConnections(ctx context.Context, workspaceID, itemID string) ([]*ItemConnection, error) {
	var all []*ItemConnection
	base := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/items/" + url.PathEscape(itemID) + "/connections"
	err := c.forEachPage(ctx, base, func(body json.RawMessage) (string, error) {
		var page itemConnectionsPage
		if err := json.Unmarshal(body, &page); err != nil {
			return "", errors.Wrapf(err, "decoding the connections of item %s", itemID)
		}
		for _, v := range page.Value {
			connection := &ItemConnection{ID: v.ID, DisplayName: v.DisplayName}
			if details := v.ConnectionDetails; details != nil {
				connection.Type, connection.Path = details.Type, details.Path
			}
			all = append(all, connection)
		}
		return page.ContinuationToken, nil
	})
	if err != nil {
		return nil, err
	}
	return all, nil
}

// forEachPage reads a paged listing. read decodes one page and returns its
// continuation token, empty on the last page.
func (c *APIClient) forEachPage(ctx context.Context, base string, read func(json.RawMessage) (string, error)) error {
	continuation := ""
	for {
		u := base
		if continuation != "" {
			u += "?continuationToken=" + url.QueryEscape(continuation)
		}
		var body json.RawMessage
		if err := c.get(ctx, u, &body); err != nil {
			return err
		}
		next, err := read(body)
		if err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		continuation = next
	}
}

// IsNotFound is true for an answer that the API does not find the workspace or
// the item: a 404, or a 400 for a workspace id that it does not know.
func IsNotFound(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.StatusCode == http.StatusNotFound ||
		(apiErr.StatusCode == http.StatusBadRequest && strings.Contains(apiErr.ErrorCode, "NotFound"))
}
