# MercuryStream Makefile

.PHONY: help demo demo-quick clean up down logs status reports benchmark replay replay-clean stress stress-max

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "  demo          Full demo: duplicates, drift, OOO, stress test"
	@echo "  demo-quick    Quick demo on running services"
	@echo "  up            Start all services"
	@echo "  down          Stop all services"
	@echo "  logs          Tail service logs"
	@echo "  status        Show service status"
	@echo "  reports       Generate incident reports"
	@echo "  clean         Remove incident data"
	@echo "  benchmark     Run performance benchmark"
	@echo "  replay        Isolated replay environment"
	@echo "  replay-clean  Stop replay and clean up"
	@echo "  stress        Run stress test (10s at 5000/s)"
	@echo "  stress-max    Run max throughput stress test"

demo: clean
	@docker compose up -d --build
	@sleep 8
	@docker compose ps
	@python3 tools/replay.py --file data/btcusd.jsonl \
		--rate 1000 \
		--duplicate-rate 0.05 \
		--drift-rate 0.03 \
		--shuffle-window 10 \
		--host localhost \
		--port 9001 \
		2>&1 | head -20
	@sleep 3
	@python3 tools/stress.py --rate 3000 --duration 5 --connections 2
	@sleep 2
	@python3 -m services.processor.incident.report data/incidents/ || true

demo-quick:
	@python3 tools/replay.py --file data/btcusd.jsonl \
		--rate 1000 \
		--duplicate-rate 0.05 \
		--drift-rate 0.03 \
		--shuffle-window 10 \
		--host localhost \
		--port 9001 \
		2>&1 | head -20
	@sleep 2
	@python3 tools/stress.py --rate 3000 --duration 5 --connections 2
	@sleep 2
	@python3 -m services.processor.incident.report data/incidents/ || true

up:
	@docker compose up -d --build
	@docker compose ps

down:
	@docker compose down

logs:
	@docker compose logs -f --tail=300

status:
	@docker compose ps
	@ls -la data/incidents/ || true
	@docker compose logs --tail=10 || true

reports:
	@python3 -m services.processor.incident.report data/incidents/

clean:
	@rm -rf data/incidents/*
	@mkdir -p data/incidents

benchmark:
	@docker compose up -d --build
	@sleep 5
	@python3 tools/replay.py --file data/btcusd.jsonl \
		--rate 0 \
		--host localhost \
		--port 9001 \
		2>&1 | tail -5

replay:
	@mkdir -p data/replay-incidents
	@rm -rf data/replay-incidents/*
	@docker compose --profile replay up -d processor-replay
	@sleep 3
	@python3 tools/replay.py --file data/btcusd.jsonl \
		--rate 1000 \
		--duplicate-rate 0.05 \
		--shuffle-window 10 \
		--host localhost \
		--port 9002 \
		2>&1 | head -20
	@sleep 5
	@python3 -m services.processor.incident.report data/replay-incidents/ || true

replay-clean:
	@docker compose --profile replay stop processor-replay || true
	@docker compose --profile replay rm -f processor-replay || true
	@rm -rf data/replay-incidents/*

stress:
	@docker compose up -d --build
	@sleep 3
	@python3 tools/stress.py --rate 5000 --duration 10 --connections 2

stress-max:
	@docker compose up -d --build
	@sleep 3
	@python3 tools/stress.py --rate 0 --duration 10 --connections 4
