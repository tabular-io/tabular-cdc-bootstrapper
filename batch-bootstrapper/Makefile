all: build run

build:
	docker build -t tabular-cdc-batch-bootstrapper -f Dockerfile ..

fresh:
	docker build -t tabular-cdc-batch-bootstrapper -f Dockerfile .. --no-cache

run:
	docker-compose up

test: build
	docker run -it --env-file ./.env tabular-cdc-batch-bootstrapper pipenv run pytest -v

run-interactive:
	docker run -it --env-file ./.env tabular-cdc-batch-bootstrapper sh