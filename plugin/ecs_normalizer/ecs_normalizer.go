package ecs_normalizer

import (
	"context"
	"fmt"
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
//  2. ip2region lookup → province + ISP.
//  3. Check Ristretto DNS response cache (province|isp|qname|qtype).
//  4. If miss: look up fixed subnet from Nacos-loaded sync.Map.
//  5. Inject/overwrite ECS option with the representative subnet.
//  6. Forward to next plugin (forward → PowerDNS).
//  7. Cache DNS response in Ristretto; return to client.
type ECSNormalizer struct {
	Next plugin.Handler
	cfg  *Config

	mu       sync.RWMutex
	searcher *xdb.Searcher // ip2region in-memory searcher (protected by mu)

	subnetMap          sync.Map         // "province|isp" → subnet CIDR string
	dnsCache           *ristretto.Cache // DNS response cache
	prefetchInFlight   sync.Map         // cacheKey -> struct{} (dedupe prefetch refresh)
	cacheIndex         sync.Map         // cacheKey -> *cachedResponse (for active prefetch scan)
	activePrefetchOnce sync.Once
	lastRejectWarnUnix atomic.Int64
}

type cachedResponse struct {
	msg       *dns.Msg
	expiresAt time.Time
	province  string
	isp       string
	subnet    string
	qname     string
	qtype     uint16
}

func (e *ECSNormalizer) Name() string { return "ecs_normalizer" }

func (e *ECSNormalizer) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	if len(r.Question) == 0 {
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	qname := r.Question[0].Name

	// 1. Extract client IP (prefer ECS option source address).
	state := request.Request{W: w, Req: r}
	clientIP, fromECS := extractClientIP(state, r)
	if fromECS {
		log.Infof("[%s] client_ip=%s (source=ecs)", qname, clientIP)
	} else {
		log.Infof("[%s] client_ip=%s (source=remote, no ecs subnet)", qname, clientIP)
	}

	// 2. ip2region lookup.
	e.mu.RLock()
	regionStr, err := e.searcher.SearchByStr(clientIP)
	e.mu.RUnlock()
	if err != nil || regionStr == "" {
		log.Debugf("[%s] ip2region miss for %s, passthrough", qname, clientIP)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	province, isp := parseRegion(regionStr)
	log.Infof("[%s] client=%s → province=%s isp=%s", qname, clientIP, province, isp)
	if province == "" || isp == "" {
		// Overseas or unknown — passthrough without ECS injection.
		log.Debugf("[%s] unknown region, passthrough", qname)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	// 3. Check DNS response cache.
	qtype := r.Question[0].Qtype
	cacheKey := cacheKeyFromMeta(province, isp, qname, qtype)

	if val, ok := e.dnsCache.Get(cacheKey); ok {
		if cr, ok := val.(*cachedResponse); ok {
			if !time.Now().Before(cr.expiresAt) {
				e.dnsCache.Del(cacheKey)
				e.cacheIndex.Delete(cacheKey)
			} else {
				remaining := time.Until(cr.expiresAt)
				if e.cfg.CachePrefetchMode == "request" && e.cfg.CachePrefetchAhead > 0 && remaining <= e.cfg.CachePrefetchAhead {
					if _, loaded := e.prefetchInFlight.LoadOrStore(cacheKey, struct{}{}); !loaded {
						log.Infof("[%s] ristretto cache prefetch trigger: province=%s isp=%s qtype=%d remaining=%s", qname, province, isp, qtype, remaining.Round(time.Millisecond))
						go e.prefetchCache(cacheKey, province, isp, qname, qtype)
					} else {
						log.Infof("[%s] ristretto cache hit (prefetch in-flight): province=%s isp=%s qtype=%d", qname, province, isp, qtype)
					}
				}
				log.Infof("[%s] ristretto cache hit: province=%s isp=%s qtype=%d", qname, province, isp, qtype)
				resp := cr.msg.Copy()
				resp.Id = r.Id
				w.WriteMsg(resp)
				return dns.RcodeSuccess, nil
			}
		}
	}

	// 4. Look up fixed representative subnet.
	subnetVal, ok := e.subnetMap.Load(province + "|" + isp)
	if !ok {
		// No mapping in Nacos yet — passthrough.
		log.Warningf("[%s] no subnet mapping for province=%s isp=%s, passthrough", qname, province, isp)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}
	subnet := subnetVal.(string)
	log.Infof("[%s] ECS injected: client=%s province=%s isp=%s → subnet=%s", qname, clientIP, province, isp, subnet)

	// 5. Clone request and inject ECS.
	rClone := r.Copy()
	if err := injectECS(rClone, subnet, e.cfg.PrefixLength); err != nil {
		log.Warningf("inject ECS failed (subnet=%s): %v", subnet, err)
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	// 6. Forward to next plugin.
	rec := dnstest.NewRecorder(w)
	rcode, err := plugin.NextOrFailure(e.Name(), e.Next, ctx, rec, rClone)
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

func (e *ECSNormalizer) prefetchCache(cacheKey, province, isp, qname string, qtype uint16) {
	defer e.prefetchInFlight.Delete(cacheKey)

	subnetVal, ok := e.subnetMap.Load(province + "|" + isp)
	if !ok {
		log.Warningf("[%s] prefetch skipped: no subnet mapping for province=%s isp=%s", qname, province, isp)
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
		qname:     qname,
		qtype:     qtype,
	}
	e.writeCache(cacheKey, cr, "prefetch")
}

func (e *ECSNormalizer) writeCache(cacheKey string, cr *cachedResponse, source string) {
	packed, _ := cr.msg.Pack()
	if e.dnsCache.Set(cacheKey, cr, int64(len(packed))) {
		e.cacheIndex.Store(cacheKey, cr)
		ttl := uint32(time.Until(cr.expiresAt).Seconds())
		log.Infof("[%s] ristretto cache %s write: province=%s isp=%s subnet=%s qtype=%d ttl=%d", cr.qname, source, cr.province, cr.isp, cr.subnet, cr.qtype, ttl)
	}
}

func cacheKeyFromMeta(province, isp, qname string, qtype uint16) string {
	return fmt.Sprintf("%s|%s|%s|%d", province, isp, qname, qtype)
}

func cacheKeyFromCachedResponse(cr *cachedResponse) string {
	if cr == nil {
		return ""
	}
	return cacheKeyFromMeta(cr.province, cr.isp, cr.qname, cr.qtype)
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
		log.Infof("[%s] active prefetch trigger: province=%s isp=%s qtype=%d remaining=%s", cr.qname, cr.province, cr.isp, cr.qtype, remaining.Round(time.Millisecond))
		go e.prefetchCache(cacheKey, cr.province, cr.isp, cr.qname, cr.qtype)
		return true
	})
}
