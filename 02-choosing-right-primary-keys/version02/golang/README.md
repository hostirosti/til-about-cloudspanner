# Demonstrating Hot-Spotting in Google Cloud Spanner

## Requirements to run this code

- Google Cloud Project
- Bucket in Google Cloud Storage (refer to the [GCS docs](https://cloud.google.com/storage/docs/quickstart-console) on how to create a bucket)
- Google Cloud Spanner Instance (refer to the [Cloud Spanner](https://cloud.google.com/spanner/docs/quickstart-console) docs on how to create a Cloud Spanner Instance)
- Google Cloud Container Registry (refer to the [Container Registry docs](https://cloud.google.com/container-registry/docs/quickstart) how to enable the registry)

Create a GCE instance and install all required packages:

```bash
gcloud compute instances create episode-01 --zone europe-west1-c --machine-type n1-standard-4 --scopes "https://www.googleapis.com/auth/cloud-platform" --image-project ubuntu-os-cloud --image-family ubuntu-1710
gcloud compute scp scripts/setup.sh episode-01:setup.sh --zone europe-west1-c
gcloud compute ssh episode-01 --zone europe-west1-c --command "sudo sh setup.sh"
```

## Primary Key Hot-Spotting

### Generating the simulation data

As first step we generate some data that looks like a DB backup
of a traditional RDBMS. For this example we assume a DB that didn't
use advanced features like triggers, stored procedures or user defined functions.
The sample schema doesn't use referential constraints since there is no support
for them in Cloud Spanner. There exists a lite version of referential constraints
in Cloud Spanner called interleaving which we will look at in another example.

We use [schema.v1.sql](schema.v1.sql) for this example.
To generate data according to schema v1 switch to branch episode01-v1.

```bash
git checkout -b episode01-v1
```

Then build the container and push it to your project private Container Registry:

```bash
docker build -t gcr.io/{{YOUR_PROJECT_ID}}/cloud-spanner-til-01:v1 --build-arg version=v1 .
gcloud docker -- push gcr.io/{{YOUR_PROJECT_ID}}/cloud-spanner-til-01:v1
```

To run the container create a GCE instance and run:

```bash
gcloud docker -- pull gcr.io/{{YOUR_PROJECT_ID}}/cloud-spanner-til-01:v1
docker run --env-file config.env -it gcr.io/{{YOUR_PROJECT_ID}}/cloud-spanner-til-01:v1
```

```bash
gcloud iam service-accounts create cloud-spanner-til --display-name "Cloud Spanner TIL Service Account - generated"
gcloud iam service-accounts keys create service-account.json --iam-account cloud-spanner-til@cloud-spanner-til.iam.gserviceaccount.com
gcloud projects add-iam-policy-binding cloud-spanner-til --member serviceAccount:cloud-spanner-til@cloud-spanner-til.iam.gserviceaccount.com --role roles/spanner.admin
gcloud projects add-iam-policy-binding cloud-spanner-til --member serviceAccount:cloud-spanner-til@cloud-spanner-til.iam.gserviceaccount.com --role roles/storage.objectAdmin
```

## Vendor Packaging

We use [`govendor`](https://github.com/kardianos/govendor) (`go get -u github.com/kardianos/govendor`) as the vendor package manager.