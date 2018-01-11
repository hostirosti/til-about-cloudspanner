# Today-I-learned about Cloud Spanner Video & Content Series

This repository contains source code accompanying the TIL about Cloud Spanner
Video and Content Series.

## Published Content

1. [First Steps](01-first-steps/README.md) with Cloud Spanner ([blog post](https://medium.com/@hostirosti/first-steps-with-google-cloud-spanner), [video](https://www.youtube.com/watch?v=Y9u0e5w77bI))

## Requirements to run the samples

- [Google Cloud Project](https://console.cloud.google.com)
- Google Cloud Spanner Instance (refer to the [Cloud Spanner](https://cloud.google.com/spanner/docs/quickstart-console) docs on how to create a Cloud Spanner Instance)

### Create a GCE instance and install all required packages

```bash
gcloud compute instances create cloud-spanner-til-01 --zone europe-west1-c --machine-type n1-highcpu-16 --scopes "https://www.googleapis.com/auth/cloud-platform" --image-project ubuntu-os-cloud --image-family ubuntu-1710
gcloud compute scp scripts/setup.sh cloud-spanner-til-01:setup.sh --zone europe-west1-c
gcloud compute ssh cloud-spanner-til-01 --zone europe-west1-c --command "sudo sh setup.sh"
```

## Developing and Testing locally

If you develop on your local machine it's recommended to use a service account.
To generate one that has the permissions to connect to Google Cloud Spanner and
write to Google Cloud Storage run the following commands:

```bash
export CSTIL_PROJECT=`gcloud config list --format 'value(core.project)'`
gcloud iam service-accounts create cloud-spanner-til --display-name "Cloud Spanner TIL Service Account - generated"
gcloud iam service-accounts keys create service-account.json --iam-account cloud-spanner-til@$CSTIL_PROJECT.iam.gserviceaccount.com
gcloud projects add-iam-policy-binding cloud-spanner-til --member serviceAccount:cloud-spanner-til@$CSTIL_PROJECT.iam.gserviceaccount.com --role roles/spanner.admin
gcloud projects add-iam-policy-binding cloud-spanner-til --member serviceAccount:cloud-spanner-til@$CSTIL_PROJECT.iam.gserviceaccount.com --role roles/storage.objectAdmin
```

## Contributing

Contributions to this repository are always welcome and highly encouraged.

See [CONTRIBUTING](CONTRIBUTING.md) for more information on how to get started.

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.

*This is not an official Google product*