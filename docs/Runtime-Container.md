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

*Updating the image*: Please modify the `Dockerfile` in the root directory. Then, build and upload the image with Google
Cloud Build (updating the tag, as is appropriate):

```shell
export PROJECT=<your-project-here>
export REPO=weather-tools
export IMAGE_URI=gcr.io/$PROJECT/$REPO
export TAG="0.0.0" # Please increment on every update.
# from the project root...

# dev release
gcloud builds submit . --tag "$IMAGE_URI:dev"

# release:
gcloud builds submit . --tag "$IMAGE_URI:$TAG"  && gcloud builds submit weather_mv/ --tag "$IMAGE_URI:latest"
```
