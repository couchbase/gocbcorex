package core

type clusterConfig struct {
}

type config struct {
}

type ConfigManager interface {
	ApplyConfig(*clusterConfig) (*config, bool)
}
