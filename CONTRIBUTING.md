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

```
git clone https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf.git
cd ecmwf
pip install -e .[dev]
```

Additionally, it's recommended that you add a pre-push hook to your local client.
```
cp bin/pre-push .git/hooks/
```

## Testing

```
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

## Deployment

_TODO(b/167704736): Figure out how to deploy to PubSub / Dataflow / GCP services._
