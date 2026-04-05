package ecs_normalizer

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
)

func (e *ECSNormalizer) startXDBReloadHTTP() error {
	if e.cfg.XDBReloadHTTPAddr == "" {
		return nil
	}
	path := e.cfg.XDBReloadHTTPPath
	if path == "" {
		path = "/ecs_normalizer/reload-xdb"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, e.handleXDBReload)

	srv := &http.Server{
		Addr:              e.cfg.XDBReloadHTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	ln, err := net.Listen("tcp", e.cfg.XDBReloadHTTPAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Errorf("xdb reload http server stopped: %v", err)
		}
	}()

	log.Infof("xdb reload endpoint enabled: addr=%s path=%s (loopback caller only)", e.cfg.XDBReloadHTTPAddr, path)
	return nil
}

func (e *ECSNormalizer) handleXDBReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": "method not allowed, use POST",
		})
		return
	}
	if !isLoopbackRemote(r.RemoteAddr) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": "forbidden: local caller required",
		})
		return
	}

	bytesV4, bytesV6, err := e.reloadSearchersFromDisk()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	log.Infof("xdb reload triggered via http: remote=%s path=%s bytes_v4=%d bytes_v6=%d", r.RemoteAddr, r.URL.Path, bytesV4, bytesV6)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"xdb_v4_path": e.cfg.IP2RegionXDB,
		"xdb_v6_path": e.cfg.IP2RegionXDBV6,
		"bytes_v4":    bytesV4,
		"bytes_v6":    bytesV6,
		"reloaded_at": time.Now().Format(time.RFC3339),
	})
}

func (e *ECSNormalizer) reloadSearchersFromDisk() (int, int, error) {
	searcherV4, bytesV4, err := loadSearcherFromPath(e.cfg.IP2RegionXDB, xdb.IPv4)
	if err != nil {
		return 0, 0, fmt.Errorf("load ipv4 ip2region xdb: %w", err)
	}

	var searcherV6 *xdb.Searcher
	bytesV6 := 0
	if e.cfg.IP2RegionXDBV6 != "" {
		searcherV6, bytesV6, err = loadSearcherFromPath(e.cfg.IP2RegionXDBV6, xdb.IPv6)
		if err != nil {
			searcherV4.Close()
			return 0, 0, fmt.Errorf("load ipv6 ip2region xdb: %w", err)
		}
	}

	e.mu.Lock()
	oldV4 := e.searcherV4
	oldV6 := e.searcherV6
	e.searcherV4 = searcherV4
	e.searcherV6 = searcherV6
	e.mu.Unlock()

	if oldV4 != nil {
		oldV4.Close()
	}
	if oldV6 != nil {
		oldV6.Close()
	}

	return bytesV4, bytesV6, nil
}

func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}
