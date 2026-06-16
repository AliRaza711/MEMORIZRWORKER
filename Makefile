# ─────────────────────────────────────────────
# Memorizr Worker — Makefile
# ─────────────────────────────────────────────

.PHONY: build up down run logs restart bash tidy

# ── Docker lifecycle ──────────────────────────

build:
	docker compose build

up:
	docker compose up -d
	@echo "✓ Memorizr worker and PostgreSQL are running."

down:
	docker compose down -v
	@echo "✓ All containers stopped and volumes removed."

restart:
	@$(MAKE) down
	@$(MAKE) up

logs:
	docker compose logs -f

bash:
	docker compose exec worker sh

# ── Local development ─────────────────────────

tidy:
	go mod tidy

run:
	go run cmd/worker/main.go
