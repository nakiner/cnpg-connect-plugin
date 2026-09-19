GO ?= go
HELM ?= helm
BUF ?= buf
TOOLS_DIR ?= $(CURDIR)/work/bin
GOVULNCHECK_VERSION := v1.8.0
IMAGE ?= cnpg-connect-plugin:0.1.0-dev

.PHONY: build test race vet fmt check-fmt chart-check check image generate vuln
build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/cnpg-connect-plugin ./cmd/cnpg-connect-plugin

test:
	$(GO) test ./...

# Scan both source reachability and the actual executable produced by build.
vuln: build
	mkdir -p $(TOOLS_DIR)
	GOBIN=$(abspath $(TOOLS_DIR)) $(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	$(TOOLS_DIR)/govulncheck ./...
	$(TOOLS_DIR)/govulncheck -mode=binary bin/cnpg-connect-plugin

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w api internal cmd test

check-fmt:
	@test -z "$$(gofmt -l api internal cmd test)" || { gofmt -l api internal cmd test; exit 1; }

chart-check:
	scripts/check-chart.sh

check: check-fmt vet test chart-check

image:
	docker build -t $(IMAGE) .

# Buf v1.25+; generated protobuf bindings are checked into the repository.
generate:
	mkdir -p $(TOOLS_DIR)
	GOBIN=$(abspath $(TOOLS_DIR)) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	GOBIN=$(abspath $(TOOLS_DIR)) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	$(BUF) lint
	PATH="$(abspath $(TOOLS_DIR)):$$PATH" $(BUF) generate
