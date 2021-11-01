# Contributing Guidelines

## Project Structure 
    
```
bin/  # Tools for development

configs/  # Save example and general purpose configs.

docs/  # Sphinx documentation, deployed to GitLab pages.

weather_dl/  # Main python package, other pipelines can come later.
    __init__.py  
    download_pipeline/  # sources for pipeline
        __init__.py 
        ...  
        <module>.py
        <module>_test.py  # We'll follow this naming pattern for tests.
    weather-dl  # script to run pipeline
    setup.py  # packages sources for execution in beam worker
              # pipeline-specific requirements managed here
    
setup.py  # Project is pip-installable, project requirements managed here.
```

## Developer Installation

- Set up a Python development environment. We recommend *pyenv* and *virtualenv*. There are plenty of guides on the Internet for setting up your environment depending on your Operating System and choice of package manager.

- Use Python version 3.8.5 for development. (Python 3.9.x is incompatible per b/173798232)

- Clone the repo and install dependencies.

  ```shell
  # clone with HTTPS
  git clone https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf.git
  # close with SSH
  git clone git@gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf.git 
  cd ecmwf
  pip install -e ."[dev]"
  ```

- Visit [our documentation on ECMWF Configuration](Configuration.md) to be able to connect to ECMWF and retrieve data.

- Install `gcloud`, the Google Cloud CLI following these [instructions](https://cloud.google.com/sdk/docs/install).

- Run `gcloud auth application-default login`. Make sure your account has *write* permissions to the storage bucket and the permissions to create a Dataflow job.

- Make sure that both Dataflow and Cloud Storage are enabled on your Google Cloud Platform project.

- Add a post-push hook to your local client.
  
  ```
  cp bin/post-push .git/hooks/
  ```

## Testing

```shell
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
