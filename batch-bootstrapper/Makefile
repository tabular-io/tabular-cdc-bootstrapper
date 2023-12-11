all: build run

build:
	docker build -t tabular-cdc-batch-bootstrapper .

fresh:
	docker build -t tabular-cdc-batch-bootstrapper --no-cache .

run:
	docker-compose up

test: build
	docker run -t --env-file .env tabular-cdc-batch-bootstrapper pipenv run pytest -v -n 4

run-interactive:
	docker run -it --env-file .env tabular-cdc-batch-bootstrapper sh