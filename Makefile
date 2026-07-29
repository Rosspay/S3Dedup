APP := s3-dedup
CMD := ./cmd/s3-dedup
DIST := bin
GO := go

LINUX_BIN := $(DIST)/$(APP)-linux-amd64
WINDOWS_BIN := $(DIST)/$(APP).exe
DEMO_COMPOSE := docker compose -f compose.demo.yaml

ifeq ($(OS),Windows_NT)
MKDIR = if not exist "$(DIST)" mkdir "$(DIST)"
CLEAN = if exist "$(DIST)" rmdir /S /Q "$(DIST)"
BUILD_LINUX = set "CGO_ENABLED=0" && set "GOOS=linux" && set "GOARCH=amd64" && $(GO) build -trimpath -o "$(LINUX_BIN)" $(CMD)
BUILD_WINDOWS = set "CGO_ENABLED=0" && set "GOOS=windows" && set "GOARCH=amd64" && $(GO) build -trimpath -o "$(WINDOWS_BIN)" $(CMD)
else
MKDIR = mkdir -p "$(DIST)"
CLEAN = rm -rf "$(DIST)"
BUILD_LINUX = CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o "$(LINUX_BIN)" $(CMD)
BUILD_WINDOWS = CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -trimpath -o "$(WINDOWS_BIN)" $(CMD)
endif

.PHONY: test build build-linux build-windows demo demo-clean clean

test:
	$(GO) test ./...
	$(GO) vet ./...

build: build-linux build-windows

build-linux:
	$(MKDIR)
	$(BUILD_LINUX)

build-windows:
	$(MKDIR)
	$(BUILD_WINDOWS)

demo:
	$(DEMO_COMPOSE) down --volumes --remove-orphans
	$(DEMO_COMPOSE) up --build --abort-on-container-exit --exit-code-from demo
	$(DEMO_COMPOSE) down --volumes --remove-orphans

demo-clean:
	$(DEMO_COMPOSE) down --volumes --remove-orphans

clean:
	$(CLEAN)
