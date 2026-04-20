# coredns-ecs (Standalone)

Standalone CoreDNS binary with `ecs_normalizer` plugin.

## Quick start

```bash
make build
sudo make install
sudo make start
```

## Service files

- Binary: `/usr/local/bin/coredns-ecs`
- Config: `/etc/coredns-ecs/Corefile`
- Systemd: `/etc/systemd/system/coredns-ecs.service`
- Multi-instance template: `/etc/systemd/system/coredns-ecs@.service`

## Release package

```bash
make release-package
make release-checksum
```

If Jenkins supplies a custom `Corefile`, pass it as `COREFILE_SRC=/path/to/Corefile` when running `make release-package` or `make install`.

Custom tag:

```bash
make release-package RELEASE_TAG=v20260405-r1
```

## Custom metrics (Prometheus)

When `prometheus` plugin is enabled, `ecs_normalizer` exports:

- `coredns_ecs_normalizer_requests_total{rcode,cache_hit}`
- `coredns_ecs_normalizer_request_duration_seconds_bucket{rcode,cache_hit}`
- `coredns_ecs_normalizer_success_total`
- `coredns_ecs_normalizer_nxdomain_total`

Example PromQL:

```promql
# Success rate (NOERROR / all)
sum(rate(coredns_ecs_normalizer_success_total[5m]))
/
sum(rate(coredns_ecs_normalizer_requests_total[5m]))

# NXDOMAIN rate
sum(rate(coredns_ecs_normalizer_nxdomain_total[5m]))
/
sum(rate(coredns_ecs_normalizer_requests_total[5m]))

# Latency p50 / p90 / p99
histogram_quantile(0.50, sum(rate(coredns_ecs_normalizer_request_duration_seconds_bucket[5m])) by (le))
histogram_quantile(0.90, sum(rate(coredns_ecs_normalizer_request_duration_seconds_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(coredns_ecs_normalizer_request_duration_seconds_bucket[5m])) by (le))
```
