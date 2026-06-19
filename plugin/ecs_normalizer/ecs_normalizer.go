package ecs_normalizer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/request"
	"github.com/dgraph-io/ristretto"
	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
	"github.com/miekg/dns"
)

// ECSNormalizer is a CoreDNS plugin that normalizes EDNS Client Subnet (ECS)
// options by mapping client IPs to fixed representative subnets per province+ISP.
//
// Flow per request:
//  1. Extract client IP from ECS option (or fallback to connection IP).
//  2. If convergence disabled: cache/forward with original ECS untouched.
//  3. Otherwise ip2region lookup → province + ISP.
//  4. Check Ristretto DNS response cache.
//  5. If miss: look up fixed representative subnet from Nacos-loaded sync.Map.
//  6. Inject/overwrite ECS option with the representative subnet.
//  7. Forward to next plugin (forward → PowerDNS), cache response, return.
type ECSNormalizer struct {
	Next plugin.Handler
	cfg  *Config

	mu         sync.RWMutex
	searcherV4 *xdb.Searcher // ip2region v4 in-memory searcher (protected by mu)
	searcherV6 *xdb.Searcher // ip2region v6 in-memory searcher (protected by mu)

	subnetMapV4        sync.Map         // "province|isp" → subnet CIDR string
	subnetMapV6        sync.Map         // "province|isp" → subnet CIDR string
	dnsCache           *ristretto.Cache // DNS response cache
	prefetchInFlight   sync.Map         // cacheKey -> struct{} (dedupe prefetch refresh)
	cacheIndex         sync.Map         // cacheKey -> *cachedResponse (for active prefetch scan)
	activePrefetchOnce sync.Once
	lastRejectWarnUnix atomic.Int64
}

const traceLogPrefix = "[ECS_TRACE]"

type cachedResponse struct {
	msg       *dns.Msg
	expiresAt time.Time
	province  string
	isp       string
	subnet    string
	fromECS   bool
	ipFamily  int
	qname     string
	qtype     uint16
}

func (e *ECSNormalizer) Name() string { return "ecs_normalizer" }

func (e *ECSNormalizer) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (rcode int, err error) {
	start := time.Now()
	cacheHit := false
	defer func() {
		e.observeRequestMetrics(time.Since(start), rcode, err, cacheHit)
	}()

	if len(r.Question) == 0 {
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	qname := r.Question[0].Name

	// 1. Extract client IP (prefer ECS option source address).
	state := request.Request{W: w, Req: r}
	clientIP, clientSubnet, fromECS := extractClientIP(state, r)
	if fromECS {
		log.Infof("%s [%s] client_ip=%s client_subnet=%s (source=ecs)", traceLogPrefix, qname, clientIP, clientSubnet)
	} else {
		log.Infof("%s [%s] client_ip=%s (source=remote, no ecs subnet)", traceLogPrefix, qname, clientIP)
	}

	ipFamily := detectIPFamily(clientIP)
	if ipFamily == 0 {
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	qtype := r.Question[0].Qtype

	if !e.cfg.EnableConvergence {
		if regionStr, regionErr := e.searchRegion(clientIP, ipFamily); regionErr == nil && regionStr != "" {
			province, isp := parseRegion(regionStr)
			if province != "" || isp != "" {
				log.Infof("%s [%s] observed client=%s -> province=%s isp=%s (not used for routing)", traceLogPrefix, qname, clientIP, province, isp)
			}
		}

		cacheKey := passthroughCacheKey(ipFamily, clientSubnet, qname, qtype)
		if val, ok := e.dnsCache.Get(cacheKey); ok {
			if cr, ok := val.(*cachedResponse); ok {
				if !time.Now().Before(cr.expiresAt) {
					e.dnsCache.Del(cacheKey)
					e.cacheIndex.Delete(cacheKey)
				} else {
					remaining := time.Until(cr.expiresAt)
					if e.cfg.CachePrefetchMode == "request" && e.cfg.CachePrefetchAhead > 0 && remaining <= e.cfg.CachePrefetchAhead {
						if _, loaded := e.prefetchInFlight.LoadOrStore(cacheKey, struct{}{}); !loaded {
							log.Infof("%s [%s] passthrough cache prefetch trigger: client_ip=%s client_subnet=%s qtype=%d remaining=%s", traceLogPrefix, qname, clientIP, clientSubnet, qtype, remaining.Round(time.Millisecond))
							go e.prefetchPassthroughCache(cacheKey, ipFamily, qname, qtype, clientSubnet, fromECS)
						} else {
							log.Infof("%s [%s] passthrough cache hit (prefetch in-flight): client_ip=%s client_subnet=%s key=%s qtype=%d", traceLogPrefix, qname, clientIP, clientSubnet, cacheKey, qtype)
						}
					}
					log.Infof("%s [%s] passthrough cache hit: client_ip=%s client_subnet=%s key=%s qtype=%d remaining=%s", traceLogPrefix, qname, clientIP, clientSubnet, cacheKey, qtype, remaining.Round(time.Millisecond))
					resp := cr.msg.Copy()
					resp.Id = r.Id
					cacheHit = true
					w.WriteMsg(resp)
					return dns.RcodeSuccess, nil
				}
			}
		}

		rClone := r.Copy()
		if fromECS {
			log.Infof("%s [%s] passthrough upstream with original ecs subnet=%s cache_key=%s", traceLogPrefix, qname, clientSubnet, cacheKey)
		} else {
			log.Infof("%s [%s] passthrough upstream without ecs cache_key=%s", traceLogPrefix, qname, cacheKey)
		}

		rec := dnstest.NewRecorder(w)
		rcode, err = plugin.NextOrFailure(e.Name(), e.Next, ctx, rec, rClone)
		if err != nil {
			return rcode, err
		}
		if rec.Msg != nil {
			if len(rec.Msg.Answer) > 0 {
				if ttl := getMinTTL(rec.Msg); ttl > 0 {
					cr := &cachedResponse{
						msg:       rec.Msg.Copy(),
						expiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
						subnet:    clientSubnet,
						fromECS:   fromECS,
						ipFamily:  ipFamily,
						qname:     qname,
						qtype:     qtype,
					}
					e.writeCache(cacheKey, cr, "passthrough")
				}
			}
			w.WriteMsg(rec.Msg)
		}
		return rcode, nil
	}

	// 2. ip2region lookup.
	regionStr, err := e.searchRegion(clientIP, ipFamily)
	if err != nil || regionStr == "" {
		log.Debugf("[%s] ip2region miss for %s, passthrough", qname, clientIP)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	province, isp := parseRegion(regionStr)
	log.Infof("%s [%s] client=%s → province=%s isp=%s", traceLogPrefix, qname, clientIP, province, isp)
	if province == "" || isp == "" {
		// Overseas or unknown — passthrough without ECS injection.
		log.Debugf("[%s] unknown region, passthrough", qname)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	// 3. Check DNS response cache.
	cacheKey := cacheKeyFromMeta(ipFamily, province, isp, qname, qtype)

	if val, ok := e.dnsCache.Get(cacheKey); ok {
		if cr, ok := val.(*cachedResponse); ok {
			if !time.Now().Before(cr.expiresAt) {
				e.dnsCache.Del(cacheKey)
				e.cacheIndex.Delete(cacheKey)
			} else {
				remaining := time.Until(cr.expiresAt)
				if e.cfg.CachePrefetchMode == "request" && e.cfg.CachePrefetchAhead > 0 && remaining <= e.cfg.CachePrefetchAhead {
					if _, loaded := e.prefetchInFlight.LoadOrStore(cacheKey, struct{}{}); !loaded {
						log.Infof("%s [%s] ristretto cache prefetch trigger: province=%s isp=%s qtype=%d remaining=%s", traceLogPrefix, qname, province, isp, qtype, remaining.Round(time.Millisecond))
						go e.prefetchCache(cacheKey, ipFamily, province, isp, qname, qtype)
					} else {
						log.Infof("%s [%s] ristretto cache hit (prefetch in-flight): client_ip=%s key=%s province=%s isp=%s subnet=%s qtype=%d", traceLogPrefix, qname, clientIP, cacheKey, province, isp, cr.subnet, qtype)
					}
				}
				log.Infof("%s [%s] ristretto cache hit: client_ip=%s key=%s province=%s isp=%s subnet=%s qtype=%d remaining=%s", traceLogPrefix, qname, clientIP, cacheKey, province, isp, cr.subnet, qtype, remaining.Round(time.Millisecond))
				resp := cr.msg.Copy()
				resp.Id = r.Id
				cacheHit = true
				w.WriteMsg(resp)
				return dns.RcodeSuccess, nil
			}
		}
	}

	// 4. Look up fixed representative subnet.
	subnetMap := e.subnetMapForFamily(ipFamily)
	subnetVal, ok := subnetMap.Load(province + "|" + isp)
	if !ok {
		// No mapping in Nacos yet — passthrough.
		log.Warningf("%s [%s] no subnet mapping for ip_family=%d province=%s isp=%s, passthrough", traceLogPrefix, qname, ipFamily, province, isp)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}
	subnet := subnetVal.(string)
	log.Infof("%s [%s] ECS injected: client=%s province=%s isp=%s → subnet=%s", traceLogPrefix, qname, clientIP, province, isp, subnet)

	// 5. Clone request and inject ECS.
	rClone := r.Copy()
	if err := injectECS(rClone, subnet, e.cfg.PrefixLength); err != nil {
		log.Warningf("inject ECS failed (subnet=%s): %v", subnet, err)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	// 6. Forward to next plugin.
	log.Infof("%s [%s] forward upstream with ecs subnet=%s cache_key=%s", traceLogPrefix, qname, subnet, cacheKey)
	rec := dnstest.NewRecorder(w)
	rcode, err = plugin.NextOrFailure(e.Name(), e.Next, ctx, rec, rClone)
	if err != nil {
		return rcode, err
	}

	// 7. Cache response and write back to client.
	if rec.Msg != nil {
		if len(rec.Msg.Answer) > 0 {
			if ttl := getMinTTL(rec.Msg); ttl > 0 {
				cr := &cachedResponse{
					msg:       rec.Msg.Copy(),
					expiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
					province:  province,
					isp:       isp,
					subnet:    subnet,
					fromECS:   true,
					ipFamily:  ipFamily,
					qname:     qname,
					qtype:     qtype,
				}
				e.writeCache(cacheKey, cr, "write")
			}
		}
		w.WriteMsg(rec.Msg)
	}
	return rcode, nil
}

func (e *ECSNormalizer) prefetchCache(cacheKey string, ipFamily int, province, isp, qname string, qtype uint16) {
	defer e.prefetchInFlight.Delete(cacheKey)

	subnetMap := e.subnetMapForFamily(ipFamily)
	subnetVal, ok := subnetMap.Load(province + "|" + isp)
	if !ok {
		log.Warningf("[%s] prefetch skipped: no subnet mapping for ip_family=%d province=%s isp=%s", qname, ipFamily, province, isp)
		return
	}
	subnet := subnetVal.(string)

	r := new(dns.Msg)
	r.SetQuestion(qname, qtype)
	r.RecursionDesired = true
	if err := injectECS(r, subnet, e.cfg.PrefixLength); err != nil {
		log.Warningf("[%s] prefetch inject ECS failed (subnet=%s): %v", qname, subnet, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pw := &prefetchWriter{}
	rcode, err := plugin.NextOrFailure(e.Name(), e.Next, ctx, pw, r)
	if err != nil {
		log.Warningf("[%s] prefetch downstream query failed: %v", qname, err)
		return
	}
	if rcode != dns.RcodeSuccess {
		log.Warningf("[%s] prefetch downstream rcode=%d", qname, rcode)
		return
	}

	msg := pw.Msg()
	if msg == nil || len(msg.Answer) == 0 {
		log.Debugf("[%s] prefetch no answer, skip cache refresh", qname)
		return
	}
	ttl := getMinTTL(msg)
	if ttl == 0 {
		return
	}

	cr := &cachedResponse{
		msg:       msg.Copy(),
		expiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
		province:  province,
		isp:       isp,
		subnet:    subnet,
		fromECS:   true,
		ipFamily:  ipFamily,
		qname:     qname,
		qtype:     qtype,
	}
	e.writeCache(cacheKey, cr, "prefetch")
}

func (e *ECSNormalizer) prefetchPassthroughCache(cacheKey string, ipFamily int, qname string, qtype uint16, clientSubnet string, fromECS bool) {
	defer e.prefetchInFlight.Delete(cacheKey)

	r := new(dns.Msg)
	r.SetQuestion(qname, qtype)
	r.RecursionDesired = true
	if fromECS {
		if err := injectECS(r, clientSubnet, 0); err != nil {
			log.Warningf("%s [%s] passthrough prefetch inject ECS failed (subnet=%s): %v", traceLogPrefix, qname, clientSubnet, err)
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pw := &prefetchWriter{}
	rcode, err := plugin.NextOrFailure(e.Name(), e.Next, ctx, pw, r)
	if err != nil {
		log.Warningf("%s [%s] passthrough prefetch downstream query failed: %v", traceLogPrefix, qname, err)
		return
	}
	if rcode != dns.RcodeSuccess {
		log.Warningf("%s [%s] passthrough prefetch downstream rcode=%d", traceLogPrefix, qname, rcode)
		return
	}

	msg := pw.Msg()
	if msg == nil || len(msg.Answer) == 0 {
		log.Debugf("[%s] passthrough prefetch no answer, skip cache refresh", qname)
		return
	}
	ttl := getMinTTL(msg)
	if ttl == 0 {
		return
	}

	cr := &cachedResponse{
		msg:       msg.Copy(),
		expiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
		subnet:    clientSubnet,
		fromECS:   fromECS,
		ipFamily:  ipFamily,
		qname:     qname,
		qtype:     qtype,
	}
	e.writeCache(cacheKey, cr, "passthrough-prefetch")
}

func (e *ECSNormalizer) writeCache(cacheKey string, cr *cachedResponse, source string) {
	packed, _ := cr.msg.Pack()
	if e.dnsCache.Set(cacheKey, cr, int64(len(packed))) {
		e.cacheIndex.Store(cacheKey, cr)
		ttl := uint32(time.Until(cr.expiresAt).Seconds())
		log.Infof("%s [%s] ristretto cache %s write: key=%s province=%s isp=%s subnet=%s qtype=%d ttl=%d", traceLogPrefix, cr.qname, source, cacheKey, cr.province, cr.isp, cr.subnet, cr.qtype, ttl)
	}
}

func cacheKeyFromMeta(ipFamily int, province, isp, qname string, qtype uint16) string {
	return fmt.Sprintf("%d|%s|%s|%s|%d", ipFamily, province, isp, qname, qtype)
}

func passthroughCacheKey(ipFamily int, clientSubnet, qname string, qtype uint16) string {
	if clientSubnet == "" {
		clientSubnet = "no_ecs"
	}
	return fmt.Sprintf("%d|passthrough|%s|%s|%d", ipFamily, clientSubnet, qname, qtype)
}

func cacheKeyFromCachedResponse(cr *cachedResponse) string {
	if cr == nil {
		return ""
	}
	if cr.province == "" && cr.isp == "" {
		return passthroughCacheKey(cr.ipFamily, cr.subnet, cr.qname, cr.qtype)
	}
	return cacheKeyFromMeta(cr.ipFamily, cr.province, cr.isp, cr.qname, cr.qtype)
}

func (e *ECSNormalizer) onCacheEvict(item *ristretto.Item) {
	cr, ok := item.Value.(*cachedResponse)
	if !ok || cr == nil {
		return
	}
	cacheKey := cacheKeyFromCachedResponse(cr)
	if cacheKey != "" {
		e.cacheIndex.Delete(cacheKey)
	}
}

func (e *ECSNormalizer) onCacheReject(item *ristretto.Item) {
	const warnInterval = int64(30 * time.Second)
	now := time.Now().UnixNano()
	last := e.lastRejectWarnUnix.Load()
	if now-last < warnInterval {
		return
	}
	if !e.lastRejectWarnUnix.CompareAndSwap(last, now) {
		return
	}
	log.Warningf("ristretto cache under memory pressure: write rejected (cost=%d, max_cost=%d). consider increasing cache_max_cost_mb", item.Cost, e.cfg.CacheMaxCostMB<<20)
}

func (e *ECSNormalizer) startActivePrefetchLoop() {
	if e.cfg.CachePrefetchMode != "active" {
		return
	}
	e.activePrefetchOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(e.cfg.CachePrefetchScan)
			defer ticker.Stop()
			for range ticker.C {
				e.scanAndPrefetch()
			}
		}()
		log.Infof("active cache prefetch started: scan=%s ahead=%s", e.cfg.CachePrefetchScan, e.cfg.CachePrefetchAhead)
	})
}

func (e *ECSNormalizer) scanAndPrefetch() {
	if e.cfg.CachePrefetchAhead <= 0 {
		return
	}
	now := time.Now()
	e.cacheIndex.Range(func(key, val any) bool {
		cacheKey, ok := key.(string)
		if !ok {
			return true
		}
		cr, ok := val.(*cachedResponse)
		if !ok || cr == nil {
			e.cacheIndex.Delete(cacheKey)
			return true
		}
		if !now.Before(cr.expiresAt) {
			e.cacheIndex.Delete(cacheKey)
			e.dnsCache.Del(cacheKey)
			return true
		}
		remaining := cr.expiresAt.Sub(now)
		if remaining > e.cfg.CachePrefetchAhead {
			return true
		}
		if _, loaded := e.prefetchInFlight.LoadOrStore(cacheKey, struct{}{}); loaded {
			return true
		}
		if cr.province == "" && cr.isp == "" {
			log.Infof("%s [%s] passthrough active prefetch trigger: subnet=%s qtype=%d remaining=%s", traceLogPrefix, cr.qname, cr.subnet, cr.qtype, remaining.Round(time.Millisecond))
			go e.prefetchPassthroughCache(cacheKey, cr.ipFamily, cr.qname, cr.qtype, cr.subnet, cr.fromECS)
			return true
		}
		log.Infof("[%s] active prefetch trigger: province=%s isp=%s qtype=%d remaining=%s", cr.qname, cr.province, cr.isp, cr.qtype, remaining.Round(time.Millisecond))
		go e.prefetchCache(cacheKey, cr.ipFamily, cr.province, cr.isp, cr.qname, cr.qtype)
		return true
	})
}

func detectIPFamily(ipStr string) int {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return 0
	}
	if ip.To4() != nil {
		return 4
	}
	return 6
}

func (e *ECSNormalizer) searchRegion(ip string, ipFamily int) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if ipFamily == 6 {
		if e.searcherV6 == nil {
			return "", nil
		}
		return e.searcherV6.SearchByStr(ip)
	}
	if e.searcherV4 == nil {
		return "", nil
	}
	return e.searcherV4.SearchByStr(ip)
}

func (e *ECSNormalizer) subnetMapForFamily(ipFamily int) *sync.Map {
	if ipFamily == 6 {
		return &e.subnetMapV6
	}
	return &e.subnetMapV4
}
