package ecs_normalizer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

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

// loadSubnetMaps fetches subnet maps from Nacos for both IPv4 and IPv6.
func (e *ECSNormalizer) loadSubnetMaps(client config_client.IConfigClient) error {
	if err := e.loadSubnetMapByDataID(client, e.cfg.NacosDataID, &e.subnetMapV4, "v4"); err != nil {
		return err
	}
	if e.cfg.NacosDataIDV6 != "" {
		if err := e.loadSubnetMapByDataID(client, e.cfg.NacosDataIDV6, &e.subnetMapV6, "v6"); err != nil {
			return err
		}
	}
	return nil
}

func (e *ECSNormalizer) loadSubnetMapByDataID(client config_client.IConfigClient, dataID string, target *sync.Map, label string) error {
	content, err := client.GetConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  e.cfg.NacosGroup,
	})
	if err != nil {
		return fmt.Errorf("get config (data_id=%s group=%s): %w", dataID, e.cfg.NacosGroup, err)
	}
	clearSyncMap(target)
	if content == "" {
		log.Warningf("nacos config is empty (data_id=%s group=%s) — will retry on next change",
			dataID, e.cfg.NacosGroup)
		return nil
	}
	m := make(map[string]string)
	if err := json.Unmarshal([]byte(content), &m); err != nil {
		return fmt.Errorf("unmarshal nacos config: %w", err)
	}
	for k, v := range m {
		target.Store(k, v)
	}
	log.Infof("loaded %d subnet mappings from nacos (%s)", len(m), label)
	return nil
}

// startNacosListener registers a change callback so the subnetMap is updated
// whenever subnet-manager pushes a new config to Nacos.
func (e *ECSNormalizer) startNacosListener(client config_client.IConfigClient) {
	register := func(dataID string, target *sync.Map, label string) {
		err := client.ListenConfig(vo.ConfigParam{
			DataId: dataID,
			Group:  e.cfg.NacosGroup,
			OnChange: func(_, _, _, data string) {
				newMap := make(map[string]string)
				if err := json.Unmarshal([]byte(data), &newMap); err != nil {
					log.Errorf("failed to parse nacos config update (%s): %v", label, err)
					return
				}
				clearSyncMap(target)
				for k, v := range newMap {
					target.Store(k, v)
				}
				log.Infof("subnet map hot-updated (%s): %d entries", label, len(newMap))
			},
		})
		if err != nil {
			log.Warningf("failed to register nacos config listener (%s): %v", label, err)
		}
	}

	register(e.cfg.NacosDataID, &e.subnetMapV4, "v4")
	if e.cfg.NacosDataIDV6 != "" {
		register(e.cfg.NacosDataIDV6, &e.subnetMapV6, "v6")
	}
}

func clearSyncMap(m *sync.Map) {
	m.Range(func(k, _ any) bool {
		m.Delete(k)
		return true
	})
}
