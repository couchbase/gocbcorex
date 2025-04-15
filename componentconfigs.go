package gocbcorex

import (
	"crypto/tls"
	"fmt"
	"net/http"
)

type AgentComponentConfigs struct {
	ConfigWatcherHttpConfig  ConfigWatcherHttpConfig
	ConfigWatcherMemdConfig  ConfigWatcherMemdConfig
	KvClientManagerClients   map[string]*KvClientConfig
	DcpClientManagerClients  map[string]*DcpClientManagerClientConfig
	VbucketRoutingInfo       *VbucketRoutingInfo
	QueryComponentConfig     QueryComponentConfig
	MgmtComponentConfig      MgmtComponentConfig
	SearchComponentConfig    SearchComponentConfig
	AnalyticsComponentConfig AnalyticsComponentConfig
}

func GenerateComponentConfigsFromConfig(
	config *ParsedConfig,
	networkType string,
	tlsConfig *tls.Config,
	bucketName string,
	authenticator Authenticator,
	httpTransport http.RoundTripper,
) *AgentComponentConfigs {
	clientName := fmt.Sprintf("gocbcorex/%s", buildVersion)

	netInfo := config.AddressesGroupForNetworkType(networkType)

	kvDataNodeIds := make([]string, 0, len(netInfo.Nodes))
	kvDataHosts := make(map[string]string, len(netInfo.Nodes))
	mgmtEndpoints := make(map[string]string, len(netInfo.Nodes))
	queryEndpoints := make(map[string]string, len(netInfo.Nodes))
	searchEndpoints := make(map[string]string, len(netInfo.Nodes))
	analyticsEndpoints := make(map[string]string, len(netInfo.Nodes))

	for _, node := range netInfo.Nodes {
		kvEpId := "kv" + node.NodeID
		mgmtEpId := "mg" + node.NodeID
		queryEpId := "qu" + node.NodeID
		searchEpId := "se" + node.NodeID
		analyticsEpId := "an" + node.NodeID

		if node.HasData {
			kvDataNodeIds = append(kvDataNodeIds, kvEpId)
		}

		if tlsConfig == nil {
			if node.NonSSLPorts.Kv > 0 {
				kvDataHosts[kvEpId] = fmt.Sprintf("%s:%d", node.Hostname, node.NonSSLPorts.Kv)
			}
			if node.NonSSLPorts.Mgmt > 0 {
				mgmtEndpoints[mgmtEpId] = fmt.Sprintf("http://%s:%d", node.Hostname, node.NonSSLPorts.Mgmt)
			}
			if node.NonSSLPorts.Query > 0 {
				queryEndpoints[queryEpId] = fmt.Sprintf("http://%s:%d", node.Hostname, node.NonSSLPorts.Query)
			}
			if node.NonSSLPorts.Search > 0 {
				searchEndpoints[searchEpId] = fmt.Sprintf("http://%s:%d", node.Hostname, node.NonSSLPorts.Search)
			}
			if node.NonSSLPorts.Analytics > 0 {
				analyticsEndpoints[analyticsEpId] = fmt.Sprintf("http://%s:%d", node.Hostname, node.NonSSLPorts.Analytics)
			}
		} else {
			if node.SSLPorts.Kv > 0 {
				kvDataHosts[kvEpId] = fmt.Sprintf("%s:%d", node.Hostname, node.SSLPorts.Kv)
			}
			if node.SSLPorts.Mgmt > 0 {
				mgmtEndpoints[mgmtEpId] = fmt.Sprintf("https://%s:%d", node.Hostname, node.SSLPorts.Mgmt)
			}
			if node.SSLPorts.Query > 0 {
				queryEndpoints[queryEpId] = fmt.Sprintf("https://%s:%d", node.Hostname, node.SSLPorts.Query)
			}
			if node.SSLPorts.Search > 0 {
				searchEndpoints[searchEpId] = fmt.Sprintf("https://%s:%d", node.Hostname, node.SSLPorts.Search)
			}
			if node.SSLPorts.Analytics > 0 {
				analyticsEndpoints[analyticsEpId] = fmt.Sprintf("https://%s:%d", node.Hostname, node.SSLPorts.Analytics)
			}
		}
	}

	clients := make(map[string]*KvClientConfig)
	for nodeId, addr := range kvDataHosts {
		clients[nodeId] = &KvClientConfig{
			Address:        addr,
			TlsConfig:      tlsConfig,
			ClientName:     clientName,
			SelectedBucket: bucketName,
			Authenticator:  authenticator,
		}
	}

	dcpClients := make(map[string]*DcpClientManagerClientConfig)
	for nodeId, addr := range kvDataHosts {
		dcpClients[nodeId] = &DcpClientManagerClientConfig{
			Address:       addr,
			TlsConfig:     tlsConfig,
			ClientName:    clientName,
			Authenticator: authenticator,
		}
	}

	mgmtEndpointsList := make([]string, 0, len(mgmtEndpoints))
	for _, ep := range mgmtEndpoints {
		mgmtEndpointsList = append(mgmtEndpointsList, ep)
	}

	return &AgentComponentConfigs{
		ConfigWatcherHttpConfig: ConfigWatcherHttpConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        mgmtEndpointsList,
			UserAgent:        clientName,
			Authenticator:    authenticator,
			BucketName:       bucketName,
		},
		ConfigWatcherMemdConfig: ConfigWatcherMemdConfig{
			Endpoints: kvDataNodeIds,
		},
		KvClientManagerClients:  clients,
		DcpClientManagerClients: dcpClients,
		VbucketRoutingInfo: &VbucketRoutingInfo{
			VbMap:      config.VbucketMap,
			ServerList: kvDataNodeIds,
		},
		QueryComponentConfig: QueryComponentConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        queryEndpoints,
			Authenticator:    authenticator,
		},
		MgmtComponentConfig: MgmtComponentConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        mgmtEndpoints,
			Authenticator:    authenticator,
		},
		SearchComponentConfig: SearchComponentConfig{
			HttpRoundTripper:    httpTransport,
			Endpoints:           searchEndpoints,
			Authenticator:       authenticator,
			VectorSearchEnabled: config.Features.FtsVectorSearch,
		},
		AnalyticsComponentConfig: AnalyticsComponentConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        analyticsEndpoints,
			Authenticator:    authenticator,
		},
	}
}
