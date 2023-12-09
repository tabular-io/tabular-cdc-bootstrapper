all: Dockerfile run

Dockerfile:
	docker build -t tabular-cdc-batch-bootstrapper .

run:
	docker-compose up