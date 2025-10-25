# Makefile for Go library (with modernizer)

GO ?= go
GOBIN ?= $(shell $(GO) env GOPATH)/bin
PKGS ?= $(shell $(GO) list ./...)

# List of tools and their binary names (latest versions)
TOOLS := \
	"goimports:goimports" \
	"golangci-lint:golangci-lint" \
	"gofumpt:gofumpt" \
	"staticcheck:staticcheck"

# Function to install a tool if missing
define INSTALL_TOOL
	@bin_path="$(GOBIN)/$(2)"; \
	if [ ! -f $$bin_path ]; then \
		echo "Installing/updating $(2) to latest..."; \
		$(GO) install $(1)@latest; \
	else \
		echo "$(2) already installed."; \
	fi
endef

.PHONY: all update-tools fmt fmt-fumpt lint staticcheck modernize test ci

all: test

# Install/update tools if missing
update-tools:
	@echo "Checking and installing development tools..."
	@for tool in $(TOOLS); do \
		module=$${tool%%:*}; \
		bin=$${tool##*:}; \
		bin_path="$(GOBIN)/$$bin"; \
		if [ ! -f $$bin_path ]; then \
			echo "Installing/updating $$bin to latest..."; \
			$(GO) install "$$module@latest"; \
		else \
			echo "$$bin already installed."; \
		fi; \
	done
	@echo "Tool check complete."

# Format Go code with gofmt
fmt:
	@echo "Running gofmt..."
	@find . -type f -name '*.go' ! -path "./vendor/*" -exec gofmt -s -w {} +
	@echo "gofmt done."

# Format Go code with stricter gofumpt
fmt-fumpt:
	@echo "Running gofumpt..."
	@find . -type f -name '*.go' ! -path "./vendor/*" -exec $(GOBIN)/gofumpt -w {} +
	@echo "gofumpt done."

# Run golangci-lint
lint:
	@echo "Running golangci-lint..."
	@$(GOBIN)/golangci-lint run ./...
	@echo "Linting done."

# Run staticcheck
staticcheck:
	@echo "Running staticcheck..."
	@$(GOBIN)/staticcheck ./...
	@echo "Staticcheck done."

# Run modernize to refactor code
modernize:
	@echo "Running modernize..."
	@go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	@echo "Modernization complete."

# Run all tests
test:
	@echo "Running tests..."
	@$(GO) test -v $(PKGS)
	@echo "Tests finished."

# Run all tests with coverage
coverage:
	@echo "Running tests with coverage..."
	@$(GO) test -coverprofile=coverage.out ./...
	@echo "Coverage report generated: coverage.out"

# Show coverage in terminal
coverage-report:
	@echo "Showing coverage summary..."
	@$(GO) tool cover -func=coverage.out

# Open coverage report in browser
coverage-html:
	@echo "Opening coverage report in browser..."
	@$(GO) tool cover -html=coverage.out


# CI target: format, lint, staticcheck, modernize, test
ci: fmt-fumpt lint staticcheck modernize coverage
	@echo "CI tasks finished."
