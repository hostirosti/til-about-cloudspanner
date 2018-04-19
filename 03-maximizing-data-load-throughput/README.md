# Choosing the right primary keys in Google Cloud Spanner

## Scenario

An online shop wants to migrate from their database to Cloud Spanner.
As a first step we generate some data that simulates the backup of a traditional
RDBMS. For this example we assume the DB didn't use advanced features like
triggers, stored procedures or user defined functions.
Traditional referential constraints aren't supported in Cloud Spanner.
To replace a subset of the referential constraints possible in traditional RDBMS
one can use interleaving, hierarchical nested parent-child tables and rows.
However, for simplicity, interleaving is not used in this example.

In folder [version01](version01) you find the sample code that loads data in
batches of unordered rows, spanning multiple table splits in the result leading
to contention during data load.

In folder [version02](version02) find the sample code that loads data in
batches of distinct ranges of ordered (by primary key columns) rows, spanning
fewer table splits per transaction resulting in less contention during data load
and thus higher data load throughputs.

You find details about best practices for bulk loading data into Cloud Spanner
[here](https://cloud.google.com/spanner/docs/bulk-loading).

## Loading data with unordered batches of rows -- not recommended

To simulate loading data with unordered batches of rows
you can follow the following steps.

### First ssh into your GCE instance

See root [README.md](../README.md#create-a-gce-instance-and-install-all-required-packages)
how to create the instance.

```bash
gcloud compute ssh til-about-cloudspanner --zone europe-west1-c
```

### Then checkout this repository

```bash
git clone https://github.com/hostirosti/til-about-cloudspanner
cd 03-maximizing-data-load-throughput/version01/golang
```

### Create a Google Cloud Spanner Instance

```bash
export CSTIL_PROJECT=`gcloud config list --format 'value(core.project)'`
gcloud spanner instances create til-about-cloudspanner --config regional-europe-west1 --description "TIL about Cloud Spanner" --nodes 3
```

### Build and Containerize the sample application

```bash
docker build -t gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1 --build-arg version=v1 .
```

If you build the container locally but want to run it from your GCE instance,
push it to the project private container registry first:

```bash
gcloud docker -- push gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1
```

### Copy and adjust the configuration file

Copy the [config-sample.env](version01/golang/config-sample.env) to `config.env`
and adjust the placeholders.
If you want to test a larger or smaller dataset adjust the cardinalities as well.
Make sure the Google Cloud Storage bucket exists before running the sample.
For `BATCHSIZE` use `1000` and for `PARALLELTXN` start with `50` on a 3 node
regional instance.

### Run the container

The demo application supports 4 commands.

1. `create` creates the database and schema
1. `generate` generates sample data and writes it in csv files to Google Cloud Storage
1. `load` reads csv files from Google Cloud Storage, and persist the data to your Google Cloud Spanner database
1. `reset` deletes all rows from all the tables in your demo database

If you run the code locally and use a service account you need to add `-v {PATH_TO_SERVICE_ACCOUNT}:/service-account.json` to your docker cmd.

First create the database and schema

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1 create
```

Next generate the sample data

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1 generate
```

Then load the sample data

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1 load
```

To reset the database run

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v1 reset
```

## Loading data with ordered batches of rows (distinct ranges) -- recommended

In folder [version02](version02) you find the sample code from version01 adjusted
to load rows into cache, order them by the primary key columns and splitting
them in distinct partitions of rows to minimize contention and maximize data
load throughput.

To run the improved sample follow the following steps:

### First ssh into your GCE instance

```bash
gcloud compute ssh til-about-cloudspanner --zone europe-west1-c
```

### Then checkout this repository

```bash
git clone https://github.com/hostirosti/til-about-cloudspanner
cd 03-maximizing-data-load-throughput/version02/golang
```

### Create a Google Cloud Spanner Instance

```bash
export CSTIL_PROJECT=`gcloud config list --format 'value(core.project)'`
gcloud spanner instances create til-about-cloudspanner --config regional-europe-west1 --description "TIL about Cloud Spanner" --nodes 3
```

### Build and Containerize the sample application

```bash
docker build -t gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 --build-arg version=v2 .
```

If you build the container locally but want to run it from your GCE instance, first
push it to the project private container registry:

```bash
gcloud docker -- push gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2
```

### Copy and adjust the configuration file

Copy the [config-sample.env](version02/golang/config-sample.env) to `config.env` and adjust the
placeholders.
If you want to test a larger or smaller dataset adjust the cardinalities as well.
Make sure the Google Cloud Storage bucket exists before running the sample.
For `BATCHSIZE` use `1000` and for `PARALLELTXN` start with `50` on a 3 node
regional instance.

### Run the container

The demo application supports 3 commands.

1. `create` creates the database and schema
1. `generate` generates sample data and writes it in csv files to Google Cloud Storage
1. `load` reads csv files from Google Cloud Storage, and persist the data to your Google Cloud Spanner database
1. `reset` deletes all rows from all the tables in your demo database

If you run the code locally and use a service account you need to add `-v {PATH_TO_SERVICE_ACCOUNT}:/service-account.json` to your docker cmd.

First create the database and schema:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 create
```

Next generate the sample data:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 generate
```

Then load the sample data:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 load
```

To reset the database run:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 reset
```

## Dataflow Connector Sample

In folder [dataflow-connector](dataflow-connector) you find the sample code for
using the [Apache Beam](https://beam.apache.org/)
[Cloud Spanner Dataflow connector](https://cloud.google.com/spanner/docs/dataflow-connector). The sample is
demonstrating how to load OrderLineItems into your Cloud Spanner database.

### Creating schema and generating sample data

Follow the steps from version02 to create the schema and sample data. You can
use the same database as before or create a new one dedicated for your testing
of the Cloud Spanner Dataflow connector. To create a dedicated database, simply
change the `DATABASE` value in your config.env before running

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-03:v2 create
```

### Running the sample Dataflow pipeline

To run the pipeline, go into the [dataflow-connector](dataflow-connector) folder
and execute the following statement (you need Maven and a JDK installed, see the
[setup.sh](../scripts/setup.sh) for reference). Make sure to replace the
placeholders `PROJECT`, `BUCKET`, `BUCKET_DATA_PATH` before executing:

```bash
mvn compile exec:java \
-Dexec.mainClass="com.google.cloud.tilaboutcloudspanner.BatchImportPipeline" \
-Dexec.args="--runner=DataflowRunner --project=$CSTIL_PROJECT --gcpTempLocation=gs://{BUCKET}/tmp --region=europe-west1 --bucketDataPath={BUCKET_DATA_PATH} --experiments=shuffle_mode=service --jobName=batchimport-from-csv" -Dexec.cleanupDaemonThreads=false
```

Example:

```bash
mvn compile exec:java \
-Dexec.mainClass="com.google.cloud.tilaboutcloudspanner.BatchImportPipeline" \
-Dexec.args="--runner=DataflowRunner --project=your-project-id --gcpTempLocation=gs://your-bucket-id/tmp --region=europe-west1 --bucketDataPath=gs://your-bucket-id/generated/orderlineitems-0*.csv --experiments=shuffle_mode=service --jobName=batchimport-from-csv" -Dexec.cleanupDaemonThreads=false
```

## Vendor Packaging

We use [`govendor`](https://github.com/kardianos/govendor) (`go get -u github.com/kardianos/govendor`) as the vendor package manager.

*This is not an official Google product*