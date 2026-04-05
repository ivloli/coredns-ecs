package ecs_normalizer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

func newNacosClient(cfg *Config) (config_client.IConfigClient, error) {
	idx := strings.LastIndex(cfg.NacosAddr, ":")
	if idx < 0 {
		return nil, fmt.Errorf("invalid nacos_addr %q, expected host:port", cfg.NacosAddr)
	}
	host := cfg.NacosAddr[:idx]
	port, err := strconv.ParseUint(cfg.NacosAddr[idx+1:], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid nacos port in %q: %w", cfg.NacosAddr, err)
	}

	sc := []constant.ServerConfig{
		*constant.NewServerConfig(host, port),
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(cfg.NacosNamespace),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir("/tmp/nacos/log"),
		constant.WithCacheDir("/tmp/nacos/cache"),
		constant.WithLogLevel("warn"),
		constant.WithUsername(cfg.NacosUsername),
		constant.WithPassword(cfg.NacosPassword),
	)
	return clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	})
}

// loadSubnetMap fetches the current subnet map from Nacos and stores it in e.subnetMap.
func (e *ECSNormalizer) loadSubnetMap(client config_client.IConfigClient) error {
	content, err := client.GetConfig(vo.ConfigParam{
		DataId: e.cfg.NacosDataID,
		Group:  e.cfg.NacosGroup,
	})
	if err != nil {
		return fmt.Errorf("get config (data_id=%s group=%s): %w", e.cfg.NacosDataID, e.cfg.NacosGroup, err)
	}
	if content == "" {
		log.Warningf("nacos config is empty (data_id=%s group=%s) — will retry on next change",
			e.cfg.NacosDataID, e.cfg.NacosGroup)
		return nil
	}
	m := make(map[string]string)
	if err := json.Unmarshal([]byte(content), &m); err != nil {
		return fmt.Errorf("unmarshal nacos config: %w", err)
	}
	for k, v := range m {
		e.subnetMap.Store(k, v)
	}
	log.Infof("loaded %d subnet mappings from nacos", len(m))
	return nil
}

// startNacosListener registers a change callback so the subnetMap is updated
// whenever subnet-manager pushes a new config to Nacos.
func (e *ECSNormalizer) startNacosListener(client config_client.IConfigClient) {
	err := client.ListenConfig(vo.ConfigParam{
		DataId: e.cfg.NacosDataID,
		Group:  e.cfg.NacosGroup,
		OnChange: func(_, _, _, data string) {
			newMap := make(map[string]string)
			if err := json.Unmarshal([]byte(data), &newMap); err != nil {
				log.Errorf("failed to parse nacos config update: %v", err)
				return
			}
			// Atomically swap subnetMap contents.
			e.subnetMap.Range(func(k, _ any) bool {
				e.subnetMap.Delete(k)
				return true
			})
			for k, v := range newMap {
				e.subnetMap.Store(k, v)
			}
			log.Infof("subnet map hot-updated: %d entries", len(newMap))
		},
	})
	if err != nil {
		log.Warningf("failed to register nacos config listener: %v", err)
	}
}
