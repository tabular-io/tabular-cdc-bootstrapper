all: build run

build:
	docker build -t tabular-cdc-batch-bootstrapper -f Dockerfile ..

build-fresh:
	docker build -t tabular-cdc-batch-bootstrapper -f Dockerfile .. --no-cache

run:
	docker-compose up