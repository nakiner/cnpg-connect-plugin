GO ?= go
HELM ?= helm
BUF ?= buf
TOOLS_DIR ?= $(CURDIR)/work/bin
IMAGE ?= cnpg-connect-plugin:0.1.0-dev

.PHONY: build test race vet fmt check-fmt chart-check check image generate
build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/cnpg-connect-plugin ./cmd/cnpg-connect-plugin

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w api internal cmd test

check-fmt:
	@test -z "$$(gofmt -l api internal cmd test)" || { gofmt -l api internal cmd test; exit 1; }

chart-check:
	$(HELM) lint chart
	$(HELM) template connect chart --namespace cnpg-system > /dev/null
	$(HELM) template connect chart --namespace cnpg-system -f examples/values-external.yaml > /dev/null
	$(HELM) template connect chart --namespace cnpg-system -f examples/values-existing-secrets.yaml > /dev/null
	$(HELM) template connect chart --namespace cnpg-system --set observer.kubeAPIQPS=50 --set observer.kubeAPIBurst=100 > /dev/null
	@if $(HELM) template connect chart --set tls.certManager.enabled=false >/dev/null 2>&1; then echo "Missing TLS Secrets must fail validation"; exit 1; fi
	@if $(HELM) template connect chart --set application.auth.existingSecret= >/dev/null 2>&1; then echo "Missing token Secret must fail validation"; exit 1; fi
	@if $(HELM) template connect chart --set observer.kubeAPIQPS=0 >/dev/null 2>&1; then echo "Zero API QPS must fail validation"; exit 1; fi
	@if $(HELM) template connect chart --set observer.kubeAPIBurst=0 >/dev/null 2>&1; then echo "Zero API burst must fail validation"; exit 1; fi

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
