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

In folder [version01](version01) you find [schema.v1.sql](version01/schema.v1.sql)
which uses INT64 as primary keys. Loading the generated sample data will hotspot
Cloud Spanner on the primary key with a monotonic increasing INT value.

In folder [version02](version02) you find [schema.v2.sql](version02/schema.v2.sql)
which uses STRING(32) as primary keys to store a hex encoded
[UUID v4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)).
This addresses the hotspotting from version1 by distributing the primary
key values evenly across a large keyspace.

## Primary Key Write Hot-Spotting

To simulate the write hotspotting you can follow the following steps.

### First ssh into your GCE instance

See root [README.md](../README.md#create-a-gce-instance-and-install-all-required-packages)
how to create the instance.

```bash
gcloud compute ssh til-about-cloudspanner --zone europe-west1-c
```

### Then checkout this repository

```bash
git clone https://github.com/hostirosti/til-about-cloudspanner
cd 02-choosing-right-primary-keys/version01/golang
```

### Create a Google Cloud Spanner Instance

```bash
export CSTIL_PROJECT=`gcloud config list --format 'value(core.project)'`
gcloud spanner instances create til-about-cloudspanner --config regional-europe-west1 --description "TIL about Cloud Spanner" --nodes 3
```

### Build and Containerize the sample application

```bash
docker build -t gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1 --build-arg version=v1 .
```

If you build the container locally but want to run it from your GCE instance,
push it to the project private container registry first:

```bash
gcloud docker -- push gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1
```

### Copy and adjust the configuration file

Copy the [config-sample.env](config-sample.env) to `config.env` and adjust the
placeholders.
If you want to test a larger or smaller dataset adjust the cardinalities as well.
Make sure the Google Cloud Storage bucket exists before running the sample.

### Run the container

The demo application supports 4 commands.

1. `create` creates the database and schema
1. `generate` generates sample data and writes it in csv files to Google Cloud Storage
1. `load` reads csv files from Google Cloud Storage, and persist the data to your Google Cloud Spanner database
1. `reset` deletes all rows from all the tables in your demo database

If you run the code locally and use a service account you need to add `-v {PATH_TO_SERVICE_ACCOUNT}:/service-account.json` to your docker cmd.

First create the database and schema

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1 create
```

Next generate the sample data

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1 generate
```

Then load the sample data

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1 load
```

To reset the database run

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v1 reset
```

## Using well distributed primary keys based on UUID v4

In folder [version02](version02) you find the sample code from version01 adjusted
to use [UUID v4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random))
as primary keys.

To run the improved non-hotspotting sample follow the following steps:

### First ssh into your GCE instance

```bash
gcloud compute ssh til-about-cloudspanner --zone europe-west1-c
```

### Then checkout this repository

```bash
git clone https://github.com/hostirosti/til-about-cloudspanner
cd 02-choosing-right-primary-keys/version02/golang
```

### Create a Google Cloud Spanner Instance

```bash
export CSTIL_PROJECT=`gcloud config list --format 'value(core.project)'`
gcloud spanner instances create til-about-cloudspanner --config regional-europe-west1 --description "TIL about Cloud Spanner" --nodes 3
```

### Build and Containerize the sample application

```bash
docker build -t gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2 --build-arg version=v2 .
```

If you build the container locally but want to run it from your GCE instance, first
push it to the project private container registry:

```bash
gcloud docker -- push gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2
```

### Copy and adjust the configuration file

Copy the [config-sample.env](config-sample.env) to `config.env` and adjust the
placeholders.
If you want to test a larger or smaller dataset adjust the cardinalities as well.
Make sure the Google Cloud Storage bucket exists before running the sample.

### Run the container

The demo application supports 3 commands.

1. `create` creates the database and schema
1. `generate` generates sample data and writes it in csv files to Google Cloud Storage
1. `load` reads csv files from Google Cloud Storage, and persist the data to your Google Cloud Spanner database
1. `reset` deletes all rows from all the tables in your demo database

If you run the code locally and use a service account you need to add `-v {PATH_TO_SERVICE_ACCOUNT}:/service-account.json` to your docker cmd.

First create the database and schema:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2 create
```

Next generate the sample data:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2 generate
```

Then load the sample data:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2 load
```

To reset the database run:

```bash
docker run --env-file config.env -it gcr.io/$CSTIL_PROJECT/til-about-cloudspanner-02:v2 reset
```

## Vendor Packaging

We use [`govendor`](https://github.com/kardianos/govendor) (`go get -u github.com/kardianos/govendor`) as the vendor package manager.

*This is not an official Google product*