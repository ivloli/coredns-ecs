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

	bytes, err := e.reloadSearcherFromDisk()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	log.Infof("xdb reload triggered via http: remote=%s path=%s bytes=%d", r.RemoteAddr, r.URL.Path, bytes)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"xdb_path":    e.cfg.IP2RegionXDB,
		"bytes":       bytes,
		"reloaded_at": time.Now().Format(time.RFC3339),
	})
}

func (e *ECSNormalizer) reloadSearcherFromDisk() (int, error) {
	cBuff, err := xdb.LoadContentFromFile(e.cfg.IP2RegionXDB)
	if err != nil {
		return 0, fmt.Errorf("load ip2region xdb: %w", err)
	}
	searcher, err := xdb.NewWithBuffer(xdb.IPv4, cBuff)
	if err != nil {
		return 0, fmt.Errorf("init searcher: %w", err)
	}

	e.mu.Lock()
	old := e.searcher
	e.searcher = searcher
	e.mu.Unlock()

	if old != nil {
		if closer, ok := any(old).(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	return len(cBuff), nil
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
