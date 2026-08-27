# MercuryStream Makefile

.PHONY: help demo demo-quick clean up down logs status reports replay replay-clean stress stress-max

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "  demo          Full demo: stress test with incident capture"
	@echo "  demo-quick    Quick demo on running services"
	@echo "  up            Start all services"
	@echo "  down          Stop all services"
	@echo "  logs          Tail service logs"
	@echo "  status        Show service status"
	@echo "  reports       Generate incident reports"
	@echo "  clean         Remove incident data"
	@echo "  replay        Replay captured incident (requires incident ID)"
	@echo "  replay-clean  Stop replay and clean up"
	@echo "  stress        Run stress test (10s at 5000/s)"
	@echo "  stress-max    Run max throughput stress test"

demo: clean
	@docker compose up -d --build
	@sleep 8
	@docker compose ps
	@echo "Running stress test to generate events..."
	@python3 tools/stress.py --rate 5000 --duration 10 --connections 2
	@sleep 2
	@python3 -m services.processor.incident.report data/incidents/ || true

demo-quick:
	@echo "Running stress test to generate events..."
	@python3 tools/stress.py --rate 5000 --duration 10 --connections 2
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
	@ls -la data/incidents/ 2>/dev/null || echo "No incidents yet"
	@docker compose logs --tail=10 2>/dev/null || true

reports:
	@python3 -m services.processor.incident.report data/incidents/

clean:
	@rm -rf data/incidents/*
	@mkdir -p data/incidents

# Usage: make replay ID=<incident_id>
replay:
ifndef ID
	@echo "Usage: make replay ID=<incident_id>"
	@echo "Available incidents:"
	@ls data/incidents/ 2>/dev/null || echo "  No incidents captured yet. Run 'make demo' first."
else
	@python3 tools/replay.py --file data/incidents/$(ID)/events.jsonl \
		--rate 500 \
		--host localhost \
		--port 9001
endif

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
