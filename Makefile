.PHONY: all build tidy install uninstall start stop restart status \
	multi-enable multi-start multi-stop multi-restart multi-status package \
	prepare-release build-release release-package release-checksum

APP_NAME := coredns-ecs
BIN_DIR := bin
BIN_PATH := $(BIN_DIR)/$(APP_NAME)
TARGET_OS ?= linux
TARGET_ARCH ?= amd64
RELEASE_TAG ?= $(shell git describe --always --dirty --tags 2>/dev/null || date +%Y%m%d%H%M%S)
RELEASE_DIR := release
RELEASE_NAME := $(APP_NAME)-offline-$(TARGET_OS)-$(TARGET_ARCH)-$(RELEASE_TAG)
RELEASE_PATH := $(RELEASE_DIR)/$(RELEASE_NAME)
RELEASE_BIN := $(RELEASE_PATH)/$(APP_NAME)
RELEASE_TAR := $(RELEASE_NAME).tar.gz

PREFIX ?= /usr/local
INSTALL_BIN := $(PREFIX)/bin/$(APP_NAME)
ETC_DIR ?= /etc/coredns-ecs
DATA_DIR ?= /var/lib/coredns-ecs/ip2region
IP2REGION_DIR_NAME ?= ip2region
IP2REGION_V4_XDB_NAME ?= ip2region_v4.xdb
IP2REGION_V6_XDB_NAME ?= ip2region_v6.xdb
IP2REGION_V4_SRC_PATH ?= $(IP2REGION_DIR_NAME)/$(IP2REGION_V4_XDB_NAME)
IP2REGION_V6_SRC_PATH ?= $(IP2REGION_DIR_NAME)/$(IP2REGION_V6_XDB_NAME)
IP2REGION_V4_INSTALL_PATH := $(DATA_DIR)/$(IP2REGION_V4_XDB_NAME)
IP2REGION_V6_INSTALL_PATH := $(DATA_DIR)/$(IP2REGION_V6_XDB_NAME)
SERVICE_DIR ?= /etc/systemd/system
COREDNS_INSTANCES ?= instance1 instance2

all: build

build:
	install -d -m 755 $(BIN_DIR)
	GOWORK=off go build -o $(BIN_PATH) .
	@echo "Built $(BIN_PATH)"

tidy:
	GOWORK=off go mod tidy

install:
	@set -e; \
	src_bin=""; \
	if [ -f "$(BIN_PATH)" ]; then \
		src_bin="$(BIN_PATH)"; \
	elif [ -f "$(APP_NAME)" ]; then \
		src_bin="$(APP_NAME)"; \
	else \
		$(MAKE) build; \
		src_bin="$(BIN_PATH)"; \
	fi; \
	install -m 755 "$$src_bin" $(INSTALL_BIN)
	install -d -m 755 $(ETC_DIR)
	install -d -m 755 $(DATA_DIR)
	@set -e; \
	if [ ! -f "$(IP2REGION_V4_INSTALL_PATH)" ]; then \
		if [ -f "$(IP2REGION_V4_SRC_PATH)" ]; then \
			install -m 644 "$(IP2REGION_V4_SRC_PATH)" "$(IP2REGION_V4_INSTALL_PATH)"; \
			echo "Installed $(IP2REGION_V4_XDB_NAME) from $(IP2REGION_V4_SRC_PATH)"; \
		fi; \
	fi
	@set -e; \
	if [ ! -f "$(IP2REGION_V6_INSTALL_PATH)" ]; then \
		if [ -f "$(IP2REGION_V6_SRC_PATH)" ]; then \
			install -m 644 "$(IP2REGION_V6_SRC_PATH)" "$(IP2REGION_V6_INSTALL_PATH)"; \
			echo "Installed $(IP2REGION_V6_XDB_NAME) from $(IP2REGION_V6_SRC_PATH)"; \
		fi; \
	fi
	[ -f $(ETC_DIR)/Corefile ] || install -m 644 Corefile.prod $(ETC_DIR)/Corefile
	install -m 644 coredns-ecs.service $(SERVICE_DIR)/coredns-ecs.service
	install -m 644 coredns-ecs@.service $(SERVICE_DIR)/coredns-ecs@.service
	for i in $(COREDNS_INSTANCES); do \
		install -d -m 755 $(ETC_DIR)/$$i; \
		[ -f $(ETC_DIR)/$$i/Corefile ] || install -m 644 Corefile.prod $(ETC_DIR)/$$i/Corefile; \
	done
	systemctl daemon-reload
	systemctl enable coredns-ecs
	@echo "Installed $(APP_NAME). Check $(ETC_DIR)/Corefile then start service."

uninstall:
	systemctl stop coredns-ecs 2>/dev/null || true
	systemctl disable coredns-ecs 2>/dev/null || true
	for i in $(COREDNS_INSTANCES); do systemctl stop coredns-ecs@$$i 2>/dev/null || true; done
	for i in $(COREDNS_INSTANCES); do systemctl disable coredns-ecs@$$i 2>/dev/null || true; done
	rm -f $(SERVICE_DIR)/coredns-ecs.service $(SERVICE_DIR)/coredns-ecs@.service $(INSTALL_BIN)
	systemctl daemon-reload

start:
	systemctl start coredns-ecs

stop:
	systemctl stop coredns-ecs

restart:
	systemctl restart coredns-ecs

status:
	systemctl status coredns-ecs

multi-enable:
	systemctl daemon-reload
	for i in $(COREDNS_INSTANCES); do systemctl enable coredns-ecs@$$i; done

multi-start:
	for i in $(COREDNS_INSTANCES); do systemctl start coredns-ecs@$$i; done

multi-stop:
	for i in $(COREDNS_INSTANCES); do systemctl stop coredns-ecs@$$i; done

multi-restart:
	for i in $(COREDNS_INSTANCES); do systemctl restart coredns-ecs@$$i; done

multi-status:
	for i in $(COREDNS_INSTANCES); do systemctl status coredns-ecs@$$i; done

package:
	@set -e; \
	items="Makefile go.mod go.sum main.go directives.go plugin Corefile Corefile.prod Corefile.example coredns-ecs.service coredns-ecs@.service README.md"; \
	if [ -d "$(IP2REGION_DIR_NAME)" ]; then items="$$items $(IP2REGION_DIR_NAME)"; fi; \
	tar -czf $(APP_NAME)-standalone.tar.gz $$items
	@echo "Created $(APP_NAME)-standalone.tar.gz"

prepare-release:
	install -d -m 755 $(RELEASE_PATH)

build-release: prepare-release
	GOWORK=off CGO_ENABLED=0 GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) go build -o $(RELEASE_BIN) .
	install -m 644 Corefile.prod $(RELEASE_PATH)/Corefile.prod
	install -m 644 coredns-ecs.service $(RELEASE_PATH)/coredns-ecs.service
	install -m 644 coredns-ecs@.service $(RELEASE_PATH)/coredns-ecs@.service
	install -m 644 Makefile $(RELEASE_PATH)/Makefile
	install -m 644 README.md $(RELEASE_PATH)/README.md
	install -d -m 755 $(RELEASE_PATH)/$(IP2REGION_DIR_NAME)
	@if [ -f "$(IP2REGION_V4_SRC_PATH)" ]; then \
		install -m 644 "$(IP2REGION_V4_SRC_PATH)" "$(RELEASE_PATH)/$(IP2REGION_DIR_NAME)/$(IP2REGION_V4_XDB_NAME)"; \
	fi
	@if [ -f "$(IP2REGION_V6_SRC_PATH)" ]; then \
		install -m 644 "$(IP2REGION_V6_SRC_PATH)" "$(RELEASE_PATH)/$(IP2REGION_DIR_NAME)/$(IP2REGION_V6_XDB_NAME)"; \
	fi
	@echo "Prepared release directory: $(RELEASE_PATH)"

release-package: build-release
	COPYFILE_DISABLE=1 COPY_EXTENDED_ATTRIBUTES_DISABLE=1 tar --no-xattrs -czf $(RELEASE_TAR) -C $(RELEASE_DIR) $(RELEASE_NAME)
	@echo "Created $(RELEASE_TAR)"

release-checksum: release-package
	shasum -a 256 $(RELEASE_TAR)
