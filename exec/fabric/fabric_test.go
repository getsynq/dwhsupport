package fabric

import "testing"

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
			name:        "default credential chain",
			conf:        FabricConf{Host: host, Database: db, AuthType: "default"},
			wantFedAuth: "ActiveDirectoryDefault",
		},
		{
			name:        "user-assigned managed identity",
			conf:        FabricConf{Host: host, Database: db, AuthType: "managed_identity", ClientID: "uami-cid"},
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
