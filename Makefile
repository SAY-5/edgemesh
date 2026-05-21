# EdgeMesh Makefile
.PHONY: all build sidecar meshctl proto lint test test-unit test-integration test-chaos bench bench-smoke manifest-validate docker clean tidy

GO            ?= go
GOBIN         ?= $(shell $(GO) env GOPATH)/bin
PROTOC        ?= protoc
GOLANGCI_LINT ?= $(GOBIN)/golangci-lint
KUBECONFORM   ?= kubeconform
PKG           := github.com/SAY-5/edgemesh
BIN_DIR       := bin
COVERAGE_FILE := coverage.txt

all: lint test build

build: sidecar meshctl

sidecar:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/sidecar ./cmd/sidecar

meshctl:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/meshctl ./cmd/meshctl

proto:
	$(PROTOC) \
		--proto_path=proto \
		--go_out=. --go_opt=module=$(PKG) \
		--go-grpc_out=. --go-grpc_opt=module=$(PKG) \
		proto/edgemesh/mesh.proto proto/echo/echo.proto

tidy:
	$(GO) mod tidy

lint:
	$(GOLANGCI_LINT) run ./...

test: test-unit

test-unit:
	$(GO) test -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./internal/... ./cmd/...

test-integration:
	RUN_CHAOS=1 $(GO) test -race -timeout 5m ./tests/integration/...

test-chaos:
	RUN_CHAOS=1 CHAOS_SCENARIOS=200 $(GO) test -race -timeout 15m -v ./tests/integration/...

bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./bench/...

bench-smoke:
	$(GO) test -bench=. -benchtime=1x -benchmem -run=^$$ ./bench/...

manifest-validate:
	$(KUBECONFORM) -strict -ignore-missing-schemas -summary k8s/base k8s/overlays/dev k8s/overlays/stg k8s/overlays/prod

manifest-render-validate:
	@for ov in dev stg prod; do \
		echo "rendering $$ov ..."; \
		kubectl kustomize k8s/overlays/$$ov | $(KUBECONFORM) -strict -ignore-missing-schemas -summary -; \
	done

docker:
	docker build -t edgemesh/sidecar:dev -f Dockerfile .

clean:
	rm -rf $(BIN_DIR) $(COVERAGE_FILE) coverage.html
