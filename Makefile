# =============================================================================
# kafka-stream — top-level Makefile
#
# Intentional startup order
# ─────────────────────────
#   1. make up          → ZooKeeper, Kafka, Cassandra, producer
#                         Pipeline is live and waiting. Producer sits in
#                         circuit-breaker state until the API comes up.
#
#   2. make api-up [MODE=...]   → start the API; data starts flowing.
#
#   3. make api-down    → kill the API; producer backs off automatically.
#      make api-up      → restart; producer reconnects within 30 s.
#
# Activity modes (passed as MODE= to api-up / api-restart):
#   quiet   — 2–8 s between events   (sparse, realistic quiet period)
#   normal  — 0.3–1.5 s              (default, moderate throughput)
#   burst   — 0.02–0.2 s             (heavy load)
#   flood   — 0 s                    (max throughput, no sleep)
#
# Quick reference:
#   make up                        # start infra + producer (no API)
#   make api-up MODE=flood         # open the tap in flood mode
#   make api-down                  # close the tap
#   make inject COUNT=50000        # burst 50 000 events through the running API
#   make logs                      # tail producer + api logs
#   make down                      # stop everything
# =============================================================================

MODE  ?= normal
COUNT ?= 1000

.PHONY: \
  up down \
  infra-up infra-down \
  api-up api-down api-restart \
  producer-up producer-down \
  build rebuild \
  spark-build \
  k8s-apply k8s-delete k8s-logs k8s-status k8s-scale \
  inject logs \
  install test lint format type-check pre-commit clean

# ── Pipeline (infra + producer, no API) ──────────────────────────────────────

up: ## Start ZooKeeper, Kafka, Cassandra, and producer — API excluded intentionally.
	## Data pipeline is live and waiting. Run `make api-up` when ready to open the tap.
	docker compose up zookeeper broker kafka-init cassandra producer -d

down: ## Stop and remove all containers (including API if running)
	docker compose down

# ── Infrastructure only (Kafka + ZooKeeper + Cassandra, no producer) ─────────

infra-up: ## Start Kafka, ZooKeeper, and Cassandra only (no producer, no API)
	docker compose up zookeeper broker kafka-init cassandra -d

infra-down: ## Stop infrastructure containers
	docker compose stop zookeeper broker cassandra

# ── API ───────────────────────────────────────────────────────────────────────

api-up: ## Start (or restart) the API — pass MODE=quiet|normal|burst|flood
	API_DEFAULT_MODE=$(MODE) docker compose up api -d

api-down: ## Stop the API container (producer keeps running, will reconnect when API returns)
	docker compose stop api

api-restart: ## Restart the API with a new MODE without touching infra or producer
	API_DEFAULT_MODE=$(MODE) docker compose up api --force-recreate -d

# ── Producer ──────────────────────────────────────────────────────────────────

producer-up: ## Start the producer
	docker compose up producer -d

producer-down: ## Stop the producer
	docker compose stop producer

# ── Build ─────────────────────────────────────────────────────────────────────

build: ## Build (or rebuild) application images
	docker compose build

rebuild: ## Force a full rebuild with no layer cache
	docker compose build --no-cache

spark-build: ## Build the Spark consumer image (kafka-stream-spark:latest)
	docker build -f Dockerfile.spark -t kafka-stream-spark:latest .

# ── Kubernetes ────────────────────────────────────────────────────────────────
# Requires: kubectl pointing at a running cluster (Docker Desktop K8s / kind)
# For KEDA autoscaling also run:
#   helm repo add kedacore https://kedacore.github.io/charts
#   helm install keda kedacore/keda --namespace keda --create-namespace

k8s-apply: spark-build ## Build image and apply all K8s manifests
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/deployment.yaml
	@echo "To enable autoscaling, also run: kubectl apply -f k8s/keda-scaledobject.yaml"

k8s-delete: ## Remove all K8s resources in the kafka-stream namespace
	kubectl delete namespace kafka-stream --ignore-not-found

k8s-logs: ## Tail logs from all running spark-consumer pods
	kubectl logs -n kafka-stream -l app=spark-consumer --follow --max-log-requests=10

k8s-status: ## Show pod and HPA/ScaledObject status
	@echo "=== Pods ==="
	kubectl get pods -n kafka-stream -o wide
	@echo "\n=== ScaledObject ==="
	kubectl get scaledobject -n kafka-stream 2>/dev/null || echo "(KEDA not installed)"

k8s-scale: ## Manually scale spark-consumer (usage: make k8s-scale REPLICAS=2)
	kubectl scale deployment spark-consumer -n kafka-stream --replicas=$(REPLICAS)

# ── Observability ─────────────────────────────────────────────────────────────

logs: ## Tail live logs from the producer and API
	docker compose logs -f producer api

inject: ## Burst COUNT events into the running stream (default COUNT=1000)
	@echo "Injecting $(COUNT) events..."
	@curl -s -X POST "http://localhost:8000/inject?count=$(COUNT)" | python3 -m json.tool

# ── Local dev ─────────────────────────────────────────────────────────────────

install: ## Install all dependencies (including dev) via uv
	uv sync --all-groups

test: ## Run the test suite with coverage
	uv run pytest test/ -v --cov=. --cov-report=term-missing

lint: ## Check formatting and linting (non-destructive)
	uv run ruff check .
	uv run black --check .
	uv run isort --check-only .

format: ## Auto-fix formatting and lint issues
	uv run black .
	uv run isort .
	uv run ruff check --fix .

type-check: ## Run mypy strict type checking
	uv run mypy .

pre-commit: ## Run all pre-commit hooks against every file
	uv run pre-commit run --all-files

clean: ## Remove all build artefacts and caches
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf htmlcov/ .coverage
