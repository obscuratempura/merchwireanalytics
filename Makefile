.PHONY: up migrate seed daily weekly send-test lint test

up:
docker compose -f app/infra/docker-compose.yml up

migrate:
python -m app.db.migrate

seed:
python scripts/seed.py

daily:
python -m app.jobs.daily

weekly:
python -m app.jobs.weekly

send-test:
python scripts/send_test_email.py

lint:
ruff check .

format:
ruff format .

test:
pytest
