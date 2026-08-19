.PHONY: up down ingest silver gold pipeline test test-unit

up:
	docker compose up -d --build

down:
	docker compose down

ingest:
	docker compose exec -w /home/iceberg/project spark-iceberg python3 -m src.bronze.ingest_olist

silver:
	docker compose exec -w /home/iceberg/project spark-iceberg python3 -m src.pipeline --skip-ingest --layer silver

gold:
	docker compose exec -w /home/iceberg/project spark-iceberg python3 -m src.pipeline --skip-ingest --layer gold

pipeline:
	docker compose exec -w /home/iceberg/project spark-iceberg python3 -m src.pipeline

test-unit:
	pytest -m "not integration"

test:
	pytest
