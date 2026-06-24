.PHONY: help build test format dev-up dev-down dev-logs chaos-all soak clean

help:
	@echo "Targets:"
	@echo "  build      — mvn verify (compile + tests + spotless:check, all modules)"
	@echo "  test       — mvn test"
	@echo "  format     — mvn spotless:apply (auto-format Java)"
	@echo "  dev-up     — docker compose up -d (local e2e stack per master spec §14.b)"
	@echo "  dev-down   — docker compose down -v"
	@echo "  dev-logs   — docker compose logs -f"
	@echo "  chaos-all  — run every tests/chaos/NN_*.sh scenario"
	@echo "  soak       — run the §14.e real-environment soak (tests/soak/run.sh)"
	@echo "  clean      — mvn clean"

build:
	mvn verify

test:
	mvn test

format:
	mvn spotless:apply

dev-up:
	docker compose up -d

dev-down:
	docker compose down -v

dev-logs:
	docker compose logs -f

chaos-all:
	bash scripts/run-chaos-tests.sh

soak:
	bash tests/soak/run.sh

clean:
	mvn clean
