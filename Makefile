IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64
NCPU ?= 8
MEM_GB ?= 24
STARTER ?= starter-duckdb
FIXTURES ?= $(STARTER)/fixtures
ANSWER ?= dev-answer
DATA ?= dev
OUT ?= out
SCRATCH ?= scratch
PYTHON ?= python3
GO ?= go
BIN := .build/shopping-sessions

.PHONY: build native dev run-dev run-container check-fixtures check-dev

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

native: $(BIN)

$(BIN): $(wildcard *.go) go.mod go.sum
	mkdir -p .build
	CGO_ENABLED=0 $(GO) build -trimpath -o $@ .

dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output "$(DATA)"

run-dev: native
	NCPU=$(NCPU) MEM_GB=$(MEM_GB) $(BIN) "$(DATA)" "$(OUT)"

run-container: build
	mkdir -p "$(OUT)" "$(SCRATCH)"
	docker run --rm --platform $(PLATFORM) --network none --read-only \
		--user "$$(id -u):$$(id -g)" --cpus $(NCPU) \
		--memory $(MEM_GB)g --memory-swap $(MEM_GB)g --pids-limit 4096 \
		--tmpfs /tmp:rw,size=64m -e NCPU=$(NCPU) -e MEM_GB=$(MEM_GB) \
		-v "$(abspath $(DATA)):/data:ro" -v "$(abspath $(OUT)):/output" \
		-v "$(abspath $(SCRATCH)):/scratch" $(IMAGE)

check-fixtures: native
	$(PYTHON) "$(STARTER)/scripts/run_fixtures.py" "$(FIXTURES)" -- "$(abspath $(BIN))"

check-dev:
	$(PYTHON) "$(STARTER)/scripts/compare.py" "$(OUT)" "$(ANSWER)"
