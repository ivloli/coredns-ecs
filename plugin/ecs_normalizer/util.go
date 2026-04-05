package ecs_normalizer

import (
	"net"
	"strings"
	"sync"

	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

// extractClientIP returns the IP to use for region lookup and whether it came from ECS.
// Priority: ECS source address > connection remote IP.
func extractClientIP(state request.Request, r *dns.Msg) (ip string, fromECS bool) {
	if opt := r.IsEdns0(); opt != nil {
		for _, o := range opt.Option {
			if ecs, ok := o.(*dns.EDNS0_SUBNET); ok && ecs.Address != nil {
				return ecs.Address.String(), true
			}
		}
	}
	return state.IP(), false
}

// injectECS sets (or replaces) the EDNS0_SUBNET option in r with the given subnet.
// prefixLength overrides the mask bits when non-zero and smaller than the subnet mask.
func injectECS(r *dns.Msg, cidr string, prefixLength uint8) error {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return err
	}
	ones, bits := ipNet.Mask.Size()
	if prefixLength > 0 && int(prefixLength) < ones {
		ones = int(prefixLength)
	}

	opt := r.IsEdns0()
	if opt == nil {
		opt = &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
		r.Extra = append(r.Extra, opt)
	}

	// Strip existing ECS option.
	filtered := opt.Option[:0]
	for _, o := range opt.Option {
		if o.Option() != dns.EDNS0SUBNET {
			filtered = append(filtered, o)
		}
	}

	ecs := &dns.EDNS0_SUBNET{
		Code:          dns.EDNS0SUBNET,
		SourceNetmask: uint8(ones),
		SourceScope:   0,
	}
	ip4 := ipNet.IP.To4()
	if ip4 != nil {
		ecs.Family = 1
		ecs.Address = ip4.Mask(net.CIDRMask(ones, 32))
	} else {
		ecs.Family = 2
		ecs.Address = ipNet.IP.Mask(net.CIDRMask(ones, bits))
	}

	opt.Option = append(filtered, ecs)
	return nil
}

// parseRegion parses ip2region SearchByStr result and returns normalized keys.
// Input format: "国家|省份|城市|运营商|地区码"
// e.g. "中国|广东省|广州市|中国电信|CN"
// Output: ("广东", "电信")
func parseRegion(regionStr string) (province, isp string) {
	parts := strings.Split(regionStr, "|")
	if len(parts) < 4 {
		return
	}
	province = normalizeProvince(strings.TrimSpace(parts[1]))
	isp = normalizeISP(strings.TrimSpace(parts[3]))
	return
}

// normalizeProvince strips trailing "省" / "市".
func normalizeProvince(s string) string {
	s = strings.TrimSuffix(s, "省")
	s = strings.TrimSuffix(s, "市")
	s = strings.TrimSpace(s)
	if s == "0" {
		return ""
	}
	return s
}

// normalizeISP strips "中国" and "云" — identical to subnet-manager's NormalizeISP
// so that Nacos keys written by subnet-manager always match lookups here.
func normalizeISP(s string) string {
	s = strings.ReplaceAll(s, "中国", "")
	s = strings.ReplaceAll(s, "云", "")
	s = strings.TrimSpace(s)
	if s == "0" {
		return ""
	}
	return s
}

// getMinTTL returns the minimum TTL across all answer records, or 0 if no answers.
func getMinTTL(msg *dns.Msg) uint32 {
	if len(msg.Answer) == 0 {
		return 0
	}
	min := msg.Answer[0].Header().Ttl
	for _, rr := range msg.Answer[1:] {
		if t := rr.Header().Ttl; t < min {
			min = t
		}
	}
	return min
}

type prefetchWriter struct {
	localAddr  net.Addr
	remoteAddr net.Addr

	mu  sync.RWMutex
	msg *dns.Msg
}

func (w *prefetchWriter) LocalAddr() net.Addr {
	return w.localAddr
}

func (w *prefetchWriter) RemoteAddr() net.Addr {
	return w.remoteAddr
}

func (w *prefetchWriter) WriteMsg(m *dns.Msg) error {
	w.mu.Lock()
	w.msg = m.Copy()
	w.mu.Unlock()
	return nil
}

func (w *prefetchWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (w *prefetchWriter) Close() error {
	return nil
}

func (w *prefetchWriter) TsigStatus() error {
	return nil
}

func (w *prefetchWriter) TsigTimersOnly(bool) {}

func (w *prefetchWriter) Hijack() {}

func (w *prefetchWriter) Msg() *dns.Msg {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.msg == nil {
		return nil
	}
	return w.msg.Copy()
}
