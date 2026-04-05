package main

import "github.com/coredns/coredns/core/dnsserver"

func init() {
	// Insert "ecs_normalizer" into the directive chain immediately before "forward",
	// so it runs (and optionally caches) before requests are forwarded upstream.
	for i, d := range dnsserver.Directives {
		if d == "forward" {
			dirs := make([]string, 0, len(dnsserver.Directives)+1)
			dirs = append(dirs, dnsserver.Directives[:i]...)
			dirs = append(dirs, "ecs_normalizer")
			dirs = append(dirs, dnsserver.Directives[i:]...)
			dnsserver.Directives = dirs
			return
		}
	}
	// Fallback: append at the end if "forward" is not found.
	dnsserver.Directives = append(dnsserver.Directives, "ecs_normalizer")
}
