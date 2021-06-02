# Contributing Guidelines

## Project Structure 
    
```
docs/  # Sphinx documentation, deployed to GitLab pages.

ecmwf_pipeline/  # Main python package, other packages can come later.
    __init__.py  
    ...  
    <module>.py
    <module>_test.py  # We'll follow this naming pattern for tests.
    
setup.py  # Project is pip-installable, requirements managed here.

notebooks/  # Explorations / investigations.
```

## Developer Installation

 - Set up a Python development environment. We recommend *pyenv* and *virtualenv*. There are plenty of guides on the Internet for setting up your environment depending on your Operating System and choice of package manager.

 - Use Python verison 3.8.5 for development. (Python 3.9.x is incompatible per b/173798232)

 - Clone the repo and install dependencies.

```shell script
git clone https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf.git
cd ecmwf
pip install -e ."[dev]" --use-feature=2020-resolver
```

 - The `--use-feature=2020-resolver` flag resolves install version conflicts among firestore dependencies. It's only needed until `pip`'s dependency resolution algorithm is upgraded.

 - Visit [our documentation on ECMWF Configuration](Configuration.md) to be able to connect to ECMWF and retrieve data.

 - Install `gcloud`, the Google Cloud CLI following these [instructions](https://cloud.google.com/sdk/docs/install).

 - Run `gcloud auth application-default login`. Make sure your account has write permissions to the storage bucket and the permissions to create a Dataflow job.

 - Make sure that both Dataflow and Cloud Storage are enabled on your Google Cloud Platform project.

 - Add a pre-push hook to your local client.

```
cp bin/pre-push .git/hooks/
```

### Upgrades

From time to time, the primary or development dependencies may change. When this occurs, it's safest to update your 
local dependencies like so: 

```shell script
pip install -r requirements.txt --force-reinstall --upgrade
pip install -e ."[dev]"  # this updates the local wheel of the specific versions of the project.
```

Additionally, when testing pipelines end-to-end, it's recommend to re-install the packages, despite them being installed
in editable mode. So, if you've made a code change and want to test it in a direct runner or dataflow, please run
`pip install -e .` before the run.

## Testing

```shell script
tox
```

Please review the [Beam testing docs](https://beam.apache.org/documentation/pipelines/test-your-pipeline/) for
guidance in how to write tests for the pipeline.

## Workflow

We recommend the [GitLab Flow Merge-Request](https://about.gitlab.com/blog/2016/10/25/gitlab-workflow-an-overview/#code-review-with-gitlab) model of development.

Commits will be squashed before merge into the main branch. Each MR requires one approver and for CI to pass.

This project uses GitLab CI for continuous integration (and later, deployment). On each commit to 
the main branch, we run commands from the above two sections. Visit the [pipeline dashboard](https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf/-/pipelines)
to check build history and status.
