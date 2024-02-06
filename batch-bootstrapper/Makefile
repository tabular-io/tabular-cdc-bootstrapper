all: build run

build:
	docker build -t tabular-cdc-batch-bootstrapper .

fresh:
	docker build -t tabular-cdc-batch-bootstrapper --no-cache .

run:
	docker-compose up
	
run-interactive:
	docker run -it --env-file .env tabular-cdc-batch-bootstrapper sh