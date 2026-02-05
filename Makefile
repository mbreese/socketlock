.PHONY: all clean build socketlockd

BIN_DIR := bin
APP := socketlockd
PKG := ./cmd/socketlockd

GOOS_ARCHES := darwin_amd64 darwin_arm64 linux_amd64 linux_arm64

all: socketlockd

socketlockd: build

build: $(GOOS_ARCHES:%=$(BIN_DIR)/$(APP).%)

$(BIN_DIR)/$(APP).%:
	@mkdir -p $(BIN_DIR)
	@set -e; \
	target="$*"; \
	GOOS=$${target%_*} GOARCH=$${target#*_} \
	go build -trimpath -o $(BIN_DIR)/$(APP).$${GOOS}_$${GOARCH} $(PKG)

clean:
	@rm -rf $(BIN_DIR)
