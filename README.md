# Tabular.io CDC Bootstrap Sevice
This repository is intended to bootstrap a fileloader to CDC processing pipeline for new s3 data detected within a given bucket and prefix. It's like magic 🌙✨

It leverages serverless.com as a deployment framework for the lambda function that monitors and responds to file events in S3.

This service includes:
- detection of new s3 files in a given sub folder and bucket in your AWS account
- will create tables in Iceberg when new folder paths are detected
- those new tables will then be autoconfigured as CDC log tables for processing in tabular
- The Tabular file loader service is also configured to ensure additional files in a given s3 path are ingested and processed by Tabular.


## Installation
- Install npm (or something else if you want to, I don't care)
```sh
brew install node
```
- from the bootstrapper directory (where the package.json file is), install Serverless Framework CLI and any plugins from the package.json
```sh
npm install
```
- install pipenv (if you want to execute any of the functions locally)
```sh
brew install pipenv
```
- install pipenv dependencies 
```sh
pipenv install
```

## Usage
- [configure serverless for your AWS account.](https://www.serverless.com/framework/docs/providers/aws/guide/credentials)
- update serverless.yml with your specific configs, including tabular credentials. You may also provide a `.env` file if you prefer. Place the file alongside the `serverless.yml` file in the same directory 💪
```.env
S3_BUCKET_TO_MONITOR=randy-pitcher-workspace--aws
S3_PATH_TO_MONITOR=cdc-bootstrap

TABULAR_TARGET_WAREHOUSE=enterprise_data_warehouse
TABULAR_CREDENTIAL=t-1234:123123123 # needs permission to create database in a warehouse and list all existing objects in a warehouse
TABULAR_CATALOG_URI=https://api.tabular.io/ws
```
- activate the python virtual environment with `pipenv shell`
- deploy with `npx sls deploy`. NOTE: if you want to just run `sls deploy`, install serverless globally with npm (`npm install -g serverless`)
- Rejoice 🌞