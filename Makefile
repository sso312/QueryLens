SHELL := /bin/bash

.PHONY: bootstrap smoke-local test-viz up down deploy-oci check-oci

bootstrap:
	./scripts/bootstrap_dev.sh

smoke-local:
	./scripts/smoke_local.sh

test-viz:
	cd backend/query-visualization && .venv/bin/python -m pytest -q

up:
	docker compose up -d --build

down:
	docker compose down

deploy-oci:
	./scripts/deploy-oci.sh

check-oci:
	./scripts/check-oci.sh
