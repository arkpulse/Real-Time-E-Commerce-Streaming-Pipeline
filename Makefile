# =============================================================
# Makefile — E-Commerce Streaming Pipeline
# =============================================================
# Usage: make <target>
# =============================================================

.PHONY: help install lint format test test-unit test-integration \
        docker-up docker-down docker-logs deploy-lambdas \
        glue-run tf-plan tf-apply tf-destroy clean

ENVIRONMENT ?= dev
PROCESSING_DATE ?= $(shell date -u +%Y-%m-%d)

# ── Help ───────────────────────────────────────────────────
help:
	@echo ""
	@echo "  E-Commerce Streaming Pipeline — Make Targets"
	@echo "  ============================================="
	@echo ""
	@echo "  Setup"
	@echo "    install           Install all dev dependencies"
	@echo ""
	@echo "  Code Quality"
	@echo "    lint              Run flake8 + mypy"
	@echo "    format            Auto-format with black + isort"
	@echo ""
	@echo "  Testing"
	@echo "    test              Run all tests"
	@echo "    test-unit         Run unit tests only (fast, no AWS)"
	@echo "    test-integration  Run integration tests (needs LocalStack)"
	@echo ""
	@echo "  Docker"
	@echo "    docker-up         Start all local services"
	@echo "    docker-up-dev     Start with LocalStack + Grafana"
	@echo "    docker-down       Stop all services"
	@echo "    docker-logs       Tail producer logs"
	@echo ""
	@echo "  Deploy"
	@echo "    deploy-lambdas    Package & deploy Lambda functions"
	@echo "    glue-run          Trigger Glue pipeline for PROCESSING_DATE"
	@echo ""
	@echo "  Terraform"
	@echo "    tf-plan           Terraform plan (ENVIRONMENT=dev|prod)"
	@echo "    tf-apply          Terraform apply"
	@echo "    tf-destroy        Terraform destroy (CAREFUL!)"
	@echo ""
	@echo "  Util"
	@echo "    clean             Remove build artefacts"
	@echo ""

# ── Setup ──────────────────────────────────────────────────
install:
	pip install -r requirements-dev.txt
	chmod +x scripts/*.sh

# ── Code Quality ───────────────────────────────────────────
lint:
	flake8 producer/ lambda/ glue/ data_quality/ tests/ \
		--max-line-length=120 \
		--ignore=E203,W503 \
		--exclude=__pycache__
	mypy producer/ data_quality/ --ignore-missing-imports --no-strict-optional

format:
	black producer/ lambda/ glue/ data_quality/ tests/ --line-length=120
	isort producer/ lambda/ glue/ data_quality/ tests/ --profile=black

# ── Testing ────────────────────────────────────────────────
test: test-unit

test-unit:
	pytest tests/unit/ -v \
		--cov=producer \
		--cov=data_quality \
		--cov-report=term-missing \
		--cov-fail-under=70 \
		-m "not integration"

test-integration:
	pytest tests/integration/ -v -m integration --timeout=120 -s

test-all:
	pytest tests/ -v --cov=producer --cov=data_quality --timeout=120

# ── Docker ─────────────────────────────────────────────────
docker-up:
	docker compose up -d producer

docker-up-dev:
	docker compose --profile local-dev --profile monitoring up -d

docker-down:
	docker compose --profile local-dev --profile monitoring down -v

docker-logs:
	docker compose logs -f producer

docker-build:
	docker compose build --no-cache producer

# ── Deploy ─────────────────────────────────────────────────
deploy-lambdas:
	ENVIRONMENT=$(ENVIRONMENT) bash scripts/deploy_lambdas.sh $(ENVIRONMENT)

glue-run:
	ENVIRONMENT=$(ENVIRONMENT) \
	PROCESSING_DATE=$(PROCESSING_DATE) \
	bash scripts/run_glue_pipeline.sh $(ENVIRONMENT) $(PROCESSING_DATE)

# ── Terraform ──────────────────────────────────────────────
tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan \
		-var-file=environments/$(ENVIRONMENT)/terraform.tfvars \
		-var="alert_email=$(ALERT_EMAIL)" \
		-out=tfplan

tf-apply:
	cd terraform && terraform apply tfplan

tf-destroy:
	@echo "WARNING: This will destroy ALL infrastructure in $(ENVIRONMENT)!"
	@read -p "Type the environment name to confirm: " confirm && \
		[ "$$confirm" = "$(ENVIRONMENT)" ] || (echo "Aborted." && exit 1)
	cd terraform && terraform destroy \
		-var-file=environments/$(ENVIRONMENT)/terraform.tfvars \
		-var="alert_email=$(ALERT_EMAIL)"

# ── Utility ────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name ".coverage" -delete
	find . -name "coverage.xml" -delete
	rm -rf .build/ .pytest_cache/ htmlcov/ dist/ build/ *.egg-info/
	@echo "Clean complete."
