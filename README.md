# Tabular.io CDC Bootstrap Sevice
This repository is intended to bootstrap a fileloader to CDC processing pipeline for new s3 data detected within a given bucket and prefix. It's like magic 🌙✨

It leverages docker + pyiceberg to do most of the work.

This service includes:
- detection of new s3 files in a given sub folder and bucket in your AWS account
- will create tables in Iceberg when new folder paths are detected
- those new tables will then be autoconfigured as file loader target tables for auto ingestion in Tabular
- CDC targets are also built for file loader target tables with Tabular managed CDC fully configured


## Installation
- You'll need docker. 
- building is much easier with the Makefile. Make sure you have `make` installed (mac & linux probably already have it)
- install pipenv if you want to run anything locally
```sh
brew install pipenv
```
- you could use pip if you wanted to as well
```sh
pip install pipenv # wasn't that easy?
```
- in batch-bootstrapper or sample_data_generator, run the following to install deps
```sh
pipenv install
```
- lastly, to build the docker container for batch-bootstrapper:
```sh
cd batch-bootstrapper
make build
```
- if you want to see the actual build commands, they're not rocket science -- just crack open the Makefile 

## Usage -- batch-bootstrapper
### Import Config Notes
- you must configure S3 notifications for your bucket
- the default configuration will not directly query s3 for new paths, but leverages the `system.s3_inventory_list` table in your target warehouse. This table does NOT update with contents from buckets Tabular is inactive in and it updates once every 24 hours. If you need to monitor s3 buckets outside of tabular's control OR need faster response times, please let us know and we'll spend some time implementing something better -- we have some of it written already. Use the Contact Us button on tabular.io or send an email to support@tabular.io
- if you used tabular to create your bucket with cloud formation, everything in the `/tabular/staged` path of your bucket is already wired up with notifications
- if NOT, [check out the docs section here.](https://docs.tabular.io/file-loader#event-notification-for-file-loader) It's pretty painless.
- **LASTLY and MOST IMPORTANTLY** -- make sure that the bucket you're monitoring is in the same AWS region as the warehouse you're writing to. It probably won't work if you do NOT do this, but worst case is you get serious egress charges for moving data across regions. 
- if you need any help, head to tabular.io and suplex that Contact Us button or email support@tabular.io - we got your back 😎


### Configure your bootstrapper 👢
- create a file `batch-bootstrapper/.env` with the following format.
- put your desired values in
```
S3_BUCKET_TO_MONITOR=randy-pitcher-workspace--aws--us-west-2
S3_PATH_TO_MONITOR=tabular/staged/enterprise_data_warehouse

TABULAR_TARGET_WAREHOUSE=enterprise_data_warehouse
TABULAR_CREDENTIAL=t-123:123456
TABULAR_CATALOG_URI=https://api.tabular.io/ws

TABULAR_CDC_ID_FIELD=id
TABULAR_CDC_TIMESTAMP_FIELD=loaded_at
```


### Running the bootstrapper 🤘
And finally, it's time to execute 💪. Run the following to launch this bottle rocket 🚀
```bash
cd batch-bootstrapper
make # 'make run' also works if you've already ran make build
```
- Rejoice 🌞