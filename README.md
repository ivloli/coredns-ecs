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

Custom tag:

```bash
make release-package RELEASE_TAG=v20260405-r1
```
