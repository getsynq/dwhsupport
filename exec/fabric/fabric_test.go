package fabric

import "testing"

func TestCanonicalAuthType(t *testing.T) {
	tests := map[string]string{
		// Empty stays empty (service-principal default).
		"": "",
		// Canonical values pass through.
		"service_principal": AuthTypeServicePrincipal,
		"azure_cli":         AuthTypeAzureCLI,
		"default":           AuthTypeDefault,
		"managed_identity":  AuthTypeManagedIdentity,
		// dbt-fabric spellings.
		"ServicePrincipal": AuthTypeServicePrincipal,
		"CLI":              AuthTypeAzureCLI,
		"auto":             AuthTypeDefault,
		// Microsoft ODBC "Authentication=" keyword spellings.
		"ActiveDirectoryServicePrincipal": AuthTypeServicePrincipal,
		"ActiveDirectoryAzCli":            AuthTypeAzureCLI,
		"ActiveDirectoryDefault":          AuthTypeDefault,
		"ActiveDirectoryManagedIdentity":  AuthTypeManagedIdentity,
		"ActiveDirectoryMSI":              AuthTypeManagedIdentity,
		// Azure SDK / shorthand spellings, mixed case and separators.
		"DefaultAzureCredential": AuthTypeDefault,
		"MSI":                    AuthTypeManagedIdentity,
		"az-cli":                 AuthTypeAzureCLI,
		"Managed Identity":       AuthTypeManagedIdentity,
		// Unrecognized values are returned unchanged.
		"nonsense": "nonsense",
	}
	for in, want := range tests {
		if got := CanonicalAuthType(in); got != want {
			t.Errorf("CanonicalAuthType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseHostIdentity(t *testing.T) {
	const host = "lvmghcalzphu5hhdgghdffivni-6apfgq3vf4eexhtn4fl5xrwg7i.datawarehouse.fabric.microsoft.com"
	got, err := ParseHostIdentity(host)
	if err != nil {
		t.Fatalf("ParseHostIdentity: %v", err)
	}
	// Verified live: @@SERVERNAME == workspace GUID, and the Fabric REST API
	// resolves it to the "Coalesce Development" workspace under this tenant.
	if want := "8863585d-cb0b-4ecf-9ce3-318e3295156a"; got.TenantID != want {
		t.Errorf("TenantID = %q, want %q", got.TenantID, want)
	}
	if want := "43531ef0-2f75-4b08-9e6d-e157dbc6c6fa"; got.WorkspaceID != want {
		t.Errorf("WorkspaceID = %q, want %q", got.WorkspaceID, want)
	}

	if _, err := ParseHostIdentity("not-a-fabric-host"); err == nil {
		t.Error("expected error for non-Fabric host")
	}
}

func TestToMSSQLConfDefaultDatabase(t *testing.T) {
	// Empty Database defaults to master (the workspace entry point).
	got := (&FabricConf{Host: "h", AccessToken: "t"}).ToMSSQLConf()
	if got.Database != "master" {
		t.Errorf("default database = %q, want master", got.Database)
	}
	// An explicit execution database is preserved.
	got = (&FabricConf{Host: "h", Database: "WH", AccessToken: "t"}).ToMSSQLConf()
	if got.Database != "WH" {
		t.Errorf("database = %q, want WH", got.Database)
	}
}

func TestToMSSQLConf(t *testing.T) {
	const host = "ws.datawarehouse.fabric.microsoft.com"
	const db = "WH"

	tests := []struct {
		name        string
		conf        FabricConf
		wantFedAuth string
		wantToken   string
		wantUser    string
		wantPass    string
	}{
		{
			name:        "service principal (default AuthType)",
			conf:        FabricConf{Host: host, Database: db, ClientID: "cid", ClientSecret: "secret"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid",
			wantPass:    "secret",
		},
		{
			name:        "service principal with tenant suffix",
			conf:        FabricConf{Host: host, Database: db, ClientID: "cid", ClientSecret: "secret", TenantID: "tid"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid@tid",
			wantPass:    "secret",
		},
		{
			name:        "explicit service_principal AuthType",
			conf:        FabricConf{Host: host, Database: db, AuthType: "service_principal", ClientID: "cid", ClientSecret: "secret"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid",
			wantPass:    "secret",
		},
		{
			name:        "dbt-fabric ServicePrincipal alias",
			conf:        FabricConf{Host: host, Database: db, AuthType: "ServicePrincipal", ClientID: "cid", ClientSecret: "secret"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid",
			wantPass:    "secret",
		},
		{
			name:        "Microsoft ODBC ActiveDirectoryServicePrincipal alias",
			conf:        FabricConf{Host: host, Database: db, AuthType: "ActiveDirectoryServicePrincipal", ClientID: "cid", ClientSecret: "secret"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid",
			wantPass:    "secret",
		},
		{
			name:        "unrecognized AuthType falls back to service principal",
			conf:        FabricConf{Host: host, Database: db, AuthType: "nonsense", ClientID: "cid", ClientSecret: "secret"},
			wantFedAuth: "ActiveDirectoryServicePrincipal",
			wantUser:    "cid",
			wantPass:    "secret",
		},
		{
			name:        "access token wins over everything",
			conf:        FabricConf{Host: host, Database: db, AuthType: "azure_cli", ClientID: "cid", ClientSecret: "secret", AccessToken: "tok"},
			wantFedAuth: "",
			wantToken:   "tok",
		},
		{
			name:        "azure cli (local execution)",
			conf:        FabricConf{Host: host, Database: db, AuthType: "azure_cli"},
			wantFedAuth: "ActiveDirectoryAzCli",
		},
		{
			name:        "dbt-fabric CLI alias",
			conf:        FabricConf{Host: host, Database: db, AuthType: "CLI"},
			wantFedAuth: "ActiveDirectoryAzCli",
		},
		{
			name:        "default credential chain",
			conf:        FabricConf{Host: host, Database: db, AuthType: "default"},
			wantFedAuth: "ActiveDirectoryDefault",
		},
		{
			name:        "dbt-fabric auto alias",
			conf:        FabricConf{Host: host, Database: db, AuthType: "auto"},
			wantFedAuth: "ActiveDirectoryDefault",
		},
		{
			name:        "user-assigned managed identity",
			conf:        FabricConf{Host: host, Database: db, AuthType: "managed_identity", ClientID: "uami-cid"},
			wantFedAuth: "ActiveDirectoryManagedIdentity",
			wantUser:    "uami-cid",
		},
		{
			name:        "MSI alias for managed identity",
			conf:        FabricConf{Host: host, Database: db, AuthType: "MSI", ClientID: "uami-cid"},
			wantFedAuth: "ActiveDirectoryManagedIdentity",
			wantUser:    "uami-cid",
		},
		{
			name:        "Microsoft ODBC ActiveDirectoryManagedIdentity alias",
			conf:        FabricConf{Host: host, Database: db, AuthType: "ActiveDirectoryManagedIdentity", ClientID: "uami-cid"},
			wantFedAuth: "ActiveDirectoryManagedIdentity",
			wantUser:    "uami-cid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.conf.ToMSSQLConf()
			if got.Host != host || got.Database != db {
				t.Errorf("host/db = %q/%q", got.Host, got.Database)
			}
			if got.Port != 1433 {
				t.Errorf("port = %d, want 1433", got.Port)
			}
			if got.Encrypt != "true" {
				t.Errorf("encrypt = %q, want true", got.Encrypt)
			}
			if got.FedAuth != tt.wantFedAuth {
				t.Errorf("fedAuth = %q, want %q", got.FedAuth, tt.wantFedAuth)
			}
			if got.AccessToken != tt.wantToken {
				t.Errorf("accessToken = %q, want %q", got.AccessToken, tt.wantToken)
			}
			if got.User != tt.wantUser {
				t.Errorf("user = %q, want %q", got.User, tt.wantUser)
			}
			if got.Password != tt.wantPass {
				t.Errorf("password = %q, want %q", got.Password, tt.wantPass)
			}
		})
	}
}
