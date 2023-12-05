# Tabular.io CDC Bootstrap Sevice
This repository is intended to bootstrap a fileloader to CDC processing pipeline for new s3 data detected within a given bucket and prefix. It's like magic 🌙✨

It leverages serverless.com as a deployment framework for the lambda function that monitors and responds to file events in S3.

This service includes:
- detection of new s3 files in a given sub folder and bucket in your AWS account
- will create tables in Iceberg when new folder paths are detected
- those new tables will then be autoconfigured as CDC log tables for processing in tabular
- The Tabular file loader service is also configured to ensure additional files in a given s3 path are ingested and processed by Tabular.


## Installation
- install Serverless Framework CLI.
```sh
brew install serverless 
```

## Usage
- [configure serverless for your AWS account.](https://www.serverless.com/framework/docs/providers/aws/guide/credentials)
- update serverless.yml with your specific configs, including tabular credentials
- deploy with `sls deploy`
- Rejoice 🌞