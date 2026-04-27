# Runtime Containers

_How to build & public custom Dataflow images in GCS_

*Pre-requisites*: Install the gcloud CLI ([instructions are here](https://cloud.google.com/sdk/docs/install)).

Then, log in to your cloud account:

```shell
gcloud auth login
```

> Please follow all the instructions from the CLI. This will involve running an
> auth script on your local machine, which will open a browser window to log you
> in.

Last, make sure you have adequate permissions to use Google Cloud Build (see
[IAM options here](https://cloud.google.com/build/docs/iam-roles-permissions)).
[This documentation](https://cloud.google.com/build/docs/securing-builds/configure-access-to-resources)
will help you configure your project to use Cloud Build.


## Building Docker Images

To build the Docker image for the repository using Google Cloud Build:

```shell
export PROJECT_ID=<your-project-here>
export REPO=<repo> # e.g., weather-tools
export VER=$(cat VERSION.txt)

gcloud builds submit --config=cloudbuild.yml --substitutions=_PROJECT_ID=$PROJECT_ID,_REPO=$REPO,_VER=$VER
```

This will build the image and tag it with both `weather-tools` (floating tag) and `weather-tools-$VER` (immutable tag).
