.PHONY: help build test format dev-up dev-down dev-logs chaos-all clean

help:
	@echo "Targets:"
	@echo "  build      — ./gradlew build (compile + spotlessCheck + tests, all modules)"
	@echo "  test       — ./gradlew test"
	@echo "  format     — ./gradlew spotlessApply (auto-format Java)"
	@echo "  dev-up     — docker compose up -d (local e2e stack per master spec §14.b)"
	@echo "  dev-down   — docker compose down -v"
	@echo "  dev-logs   — docker compose logs -f"
	@echo "  chaos-all  — run every tests/chaos/NN_*.sh scenario"
	@echo "  clean      — ./gradlew clean"

build:
	./gradlew build

test:
	./gradlew test

format:
	./gradlew spotlessApply

dev-up:
	docker compose up -d

dev-down:
	docker compose down -v

dev-logs:
	docker compose logs -f

chaos-all:
	bash scripts/run-chaos-tests.sh

clean:
	./gradlew clean
