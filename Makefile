.PHONY: all clean build socketlockd

BIN_DIR := bin
APP := socketlockd
PKG := ./cmd/socketlockd

GOOS_ARCHES := darwin_amd64 darwin_arm64 linux_amd64 linux_arm64

all: socketlockd

socketlockd: build

build:
	@mkdir -p $(BIN_DIR)
	@set -e; \
	for target in $(GOOS_ARCHES); do \
		GOOS=$${target%_*} GOARCH=$${target#*_} \
		go build -trimpath -o $(BIN_DIR)/$(APP).$${GOOS}_$${GOARCH} $(PKG); \
	done

clean:
	@rm -rf $(BIN_DIR)
