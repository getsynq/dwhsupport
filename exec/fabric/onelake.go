package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/pkg/errors"
)

// OneLakeBaseURL is the OneLake endpoint of the ADLS Gen2 (DFS) API. Each
// Fabric workspace is a file system of it, and each item a directory named by
// the item GUID.
const OneLakeBaseURL = "https://onelake.dfs.fabric.microsoft.com"

// oneLakeScope is the token audience of OneLake: the Azure Storage one. A token
// for the SQL endpoint or the Fabric REST API is not accepted there.
const oneLakeScope = "https://storage.azure.com/.default"

// oneLakeAPIVersion is the x-ms-version of the DFS API calls.
const oneLakeAPIVersion = "2023-11-03"

// OneLakeClient reads files of a workspace from OneLake with the connection's
// own identity. OneLake is the storage API, so its calls do not count against
// the quota of the Fabric REST API.
type OneLakeClient struct {
	cred    azcore.TokenCredential
	baseURL string
	http    *http.Client
}

// NewOneLakeClient builds a OneLake client from the same identity the SQL
// connection uses. It returns ErrNoAPICredential when the configuration holds
// only a pre-acquired SQL token.
func NewOneLakeClient(conf *FabricConf) (*OneLakeClient, error) {
	cred, err := conf.apiCredential()
	if err != nil {
		return nil, err
	}
	return NewOneLakeClientWith(cred, OneLakeBaseURL, nil), nil
}

// NewOneLakeClientWith builds a client against baseURL with an explicit
// credential and HTTP client (nil for a default one); tests point it at a fake
// server.
func NewOneLakeClientWith(cred azcore.TokenCredential, baseURL string, httpClient *http.Client) *OneLakeClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &OneLakeClient{cred: cred, baseURL: strings.TrimSuffix(baseURL, "/"), http: httpClient}
}

// OneLakePath is one entry of a directory listing.
type OneLakePath struct {
	// Name is the last segment of the path.
	Name        string
	IsDirectory bool
	// LastModified is the time the file was last written, to the second.
	LastModified  time.Time
	ContentLength int64
}

// OneLakeError is a refusal or failure answered by OneLake.
type OneLakeError struct {
	StatusCode int
	// Code is the storage error code, e.g. PathNotFound or AuthorizationPermissionMismatch.
	Code    string
	Message string
}

func (e *OneLakeError) Error() string {
	return fmt.Sprintf("onelake: %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// IsNotFound reports whether the path, the item or the workspace does not exist.
func (e *OneLakeError) IsNotFound() bool { return e.StatusCode == http.StatusNotFound }

// IsPermissionError reports whether OneLake refused the identity: 403 for an
// identity without access to the item, 401 for one it does not accept.
func (e *OneLakeError) IsPermissionError() bool {
	return e.StatusCode == http.StatusForbidden || e.StatusCode == http.StatusUnauthorized
}

// IsOneLakeNotFound reports whether err is a OneLake answer that the path does not exist.
func IsOneLakeNotFound(err error) bool {
	var lakeErr *OneLakeError
	return errors.As(err, &lakeErr) && lakeErr.IsNotFound()
}

type oneLakePathJSON struct {
	Name          string `json:"name"`
	IsDirectory   string `json:"isDirectory"`
	LastModified  string `json:"lastModified"`
	ContentLength string `json:"contentLength"`
}

// ListOptions narrows a directory listing.
type ListOptions struct {
	// BeginFrom starts the listing at the first child whose name sorts at or
	// after it. The children are in the byte order of their names.
	BeginFrom string
	// Limit ends the listing after this many children. Zero lists all of them.
	Limit int
}

// maxListResults is the largest page that the DFS API gives.
const maxListResults = 5000

// ListDirectory lists the direct children of directory, a path inside the item
// (for example "Tables/sales/orders/_delta_log"), in the order of their names.
// It follows the continuation of a listing that spans several pages.
func (c *OneLakeClient) ListDirectory(ctx context.Context, workspaceID, itemID, directory string, opts ListOptions) ([]*OneLakePath, error) {
	var all []*OneLakePath
	continuation := ""
	for {
		q := url.Values{}
		q.Set("resource", "filesystem")
		q.Set("recursive", "false")
		q.Set("directory", itemID+"/"+strings.Trim(directory, "/"))
		// The continuation holds the position, so only the first page needs beginFrom.
		if opts.BeginFrom != "" && continuation == "" {
			q.Set("beginFrom", opts.BeginFrom)
		}
		if opts.Limit > 0 {
			q.Set("maxResults", strconv.Itoa(min(opts.Limit-len(all), maxListResults)))
		}
		if continuation != "" {
			q.Set("continuation", continuation)
		}
		u := c.baseURL + "/" + url.PathEscape(workspaceID) + "?" + q.Encode()
		body, header, err := c.get(ctx, u)
		if err != nil {
			return nil, err
		}
		var page struct {
			Paths []oneLakePathJSON `json:"paths"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, errors.Wrapf(err, "decoding the listing of %s", directory)
		}
		for _, p := range page.Paths {
			path, err := p.decode()
			if err != nil {
				return nil, errors.Wrapf(err, "decoding the listing of %s", directory)
			}
			all = append(all, path)
		}
		continuation = header.Get("x-ms-continuation")
		// A page can hold fewer children than asked for, even none, and still
		// have a continuation.
		if continuation == "" || (opts.Limit > 0 && len(all) >= opts.Limit) {
			return all, nil
		}
	}
}

func (p oneLakePathJSON) decode() (*OneLakePath, error) {
	out := &OneLakePath{Name: p.Name, IsDirectory: p.IsDirectory == "true"}
	if i := strings.LastIndex(p.Name, "/"); i >= 0 {
		out.Name = p.Name[i+1:]
	}
	if p.LastModified != "" {
		t, err := http.ParseTime(p.LastModified)
		if err != nil {
			return nil, errors.Wrapf(err, "lastModified of %s", p.Name)
		}
		out.LastModified = t.UTC()
	}
	if p.ContentLength != "" {
		n, err := strconv.ParseInt(p.ContentLength, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "contentLength of %s", p.Name)
		}
		out.ContentLength = n
	}
	return out, nil
}

// ReadFile reads the whole file at path, a path inside the item.
func (c *OneLakeClient) ReadFile(ctx context.Context, workspaceID, itemID, path string) ([]byte, error) {
	segments := []string{url.PathEscape(workspaceID), url.PathEscape(itemID)}
	for _, s := range strings.Split(strings.Trim(path, "/"), "/") {
		segments = append(segments, url.PathEscape(s))
	}
	body, _, err := c.get(ctx, c.baseURL+"/"+strings.Join(segments, "/"))
	return body, err
}

func (c *OneLakeClient) get(ctx context.Context, u string) ([]byte, http.Header, error) {
	token, err := c.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{oneLakeScope}})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get a OneLake token")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	req.Header.Set("x-ms-version", oneLakeAPIVersion)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "GET %s", u)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading GET %s", u)
	}
	if resp.StatusCode != http.StatusOK {
		lakeErr := &OneLakeError{StatusCode: resp.StatusCode, Code: resp.Header.Get("x-ms-error-code")}
		var payload struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &payload) == nil {
			if payload.Error.Code != "" {
				lakeErr.Code = payload.Error.Code
			}
			lakeErr.Message = payload.Error.Message
		}
		return nil, nil, errors.WithStack(lakeErr)
	}
	return body, resp.Header, nil
}
