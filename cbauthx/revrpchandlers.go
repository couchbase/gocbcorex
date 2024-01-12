package cbauthx

type UpdateDBOptions_Node struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
	Ports    []int  `json:"ports"`
	Local    bool   `json:"local"`
}

type UpdateDBOptions_LimitsConfig struct {
	EnforceLimits     bool
	UserLimitsVersion string
}

type UpdateDBOptions_ClusterEncryptionConfig struct {
	EncryptData        bool
	DisableNonSSLPorts bool
}

type UpdateDBOptions_TlsConfig struct {
	MinTLSVersion        string
	Ciphers              []uint16
	CipherNames          []string
	CipherOpenSSLNames   []string
	CipherOrder          bool
	Present              bool
	PrivateKeyPassphrase []byte
}

// UpdateDBOptions is a structure into which the revrpc json is unmarshalled
type UpdateDBOptions struct {
	Nodes                   []UpdateDBOptions_Node
	AuthCheckURL            string                                  `json:"authCheckUrl"`
	PermissionCheckURL      string                                  `json:"permissionCheckUrl"`
	LimitsCheckURL          string                                  `json:"limitsCheckURL"`
	UuidCheckURL            string                                  `json:"uuidCheckURL"`
	SpecialUser             string                                  `json:"specialUser"`
	PermissionsVersion      string                                  `json:"permissionsVersion"`
	LimitsConfig            UpdateDBOptions_LimitsConfig            `json:"limitsConfig"`
	UserVersion             string                                  `json:"usersVersion"`
	AuthVersion             string                                  `json:"authVersion"`
	CertVersion             int                                     `json:"certVersion"`
	ExtractUserFromCertURL  string                                  `json:"extractUserFromCertURL"`
	ClientCertAuthState     string                                  `json:"clientCertAuthState"`
	ClientCertAuthVersion   string                                  `json:"clientCertAuthVersion"`
	ClusterEncryptionConfig UpdateDBOptions_ClusterEncryptionConfig `json:"clusterEncryptionConfig"`
	TLSConfig               UpdateDBOptions_TlsConfig               `json:"tlsConfig"`
}

// Cache is a structure into which the revrpc json is unmarshalled if
// used from external service
type UpdateDBExtOptions struct {
	AuthCheckEndpoint           string `json:"authCheckEndpoint"`
	AuthVersion                 string `json:"authVersion"`
	PermissionCheckEndpoint     string `json:"permissionCheckEndpoint"`
	PermissionsVersion          string `json:"permissionsVersion"`
	ExtractUserFromCertEndpoint string `json:"extractUserFromCertEndpoint"`
	ClientCertAuthVersion       string `json:"clientCertAuthVersion"`
	ClientCertAuthState         string `json:"clientCertAuthState"`
	NodeUUID                    string `json:"nodeUUID"`
	ClusterUUID                 string `json:"clusterUUID"`
}

type HeartbeatOptions struct{}

type RevRpcHandlers interface {
	Heartbeat(opts *HeartbeatOptions) (bool, error)
	UpdateDB(opts *UpdateDBOptions) (bool, error)
	UpdateDBExt(opts *UpdateDBExtOptions) (bool, error)
}
