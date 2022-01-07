# How to Contribute

We'd love to accept your patches and contributions to this project. There are just a few small guidelines you need to
follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License Agreement (CLA). You (or your employer)
retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to
<https://cla.developers.google.com/> to see your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it again.

## Code Reviews

All submissions, including submissions by project members, require review. We use GitHub pull requests for this purpose.
Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Pull Request Etiquette

We're thrilled to have your pull request! However, we ask that you file an issue (or check if one exist) before
submitting a change-list (CL).

In general, we're happier reviewing many small PRs over fewer, larger changes. Please try to
submit [small CLs](https://google.github.io/eng-practices/review/developer/small-cls.html).

## Project Structure

```
bin/  # Tools for development

configs/  # Save example and general purpose configs.

docs/  # Sphinx documentation, deployed to Github pages (or readthedocs).

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

1. Set up a Python development environment. We recommend *Anaconda*, *pyenv* or *virtualenv*.

    * Use Python version 3.8.5+ for development. (Python 3.9.x is incompatible
      with [Apache Beam's Python SDK](https://issues.apache.org/jira/browse/BEAM-12000).)

1. Clone the repo and install dependencies.

   ```shell
   # clone with HTTPS
   git clone http://github.com/google/weather-tools.git
   # close with SSH
   git clone git@github.com:google/weather-tools.git
   cd weather-tools
   pip install -e ."[dev]"
   ```

1. Install `gcloud`, the Google Cloud CLI, following these [instructions](https://cloud.google.com/sdk/docs/install).

1. Acquire adequate permissions for the project.
    * Run `gcloud auth application-default login`.

    * Make sure your account has *write* permissions to the storage bucket as well as the permissions to create a
      Dataflow job.

    * Make sure that both Dataflow and Cloud Storage are enabled on your Google Cloud Platform project.

## Testing

For testing at development time, we make use of three tools:

* `pytype` for checking types:
   ```shell
   # check everything
   pytype
   # check a specific tool
   pytype weather_dl
   ```

* `flake8` for linting:
   ```shell 
   # lint everything
   flake8
   # lint a specific tool
   flake8 weather_mv
   ```

* `pytest` for running tests:
   ```shell
   # test everything 
   pytest
   # test a specific tool
   pytest weather_sp
   ```

If you'd like to automate running these checks, we provide a post-push git hook:

```shell
cp bin/post-push .git/hooks/
```

This script can be run manually, too.

If you'd like to locally run these checks for all versions of python this project supports, you can use
`tox`. Tox will make use of the python versions installed on your machine and create virtual test environments on your
behalf.

```shell
tox
```

In addition, we provide a simple script to install _other_ branches locally. Run `bin/install-branch <branch-name>` to
pip install that branches working copy of weather-tools. Hopefully, this script facilitates testing of work-in-progress
contributions.

Please review the [Beam testing docs](https://beam.apache.org/documentation/pipelines/test-your-pipeline/) for guidance
in how to write tests for the pipeline.

## Documentation

Documents are generated with Sphinx and the myst-parser. To generate the documents locally, simply invoke `make`:

```shell
cd docs
rm -r _build
make html
```

> Note: Due to the idiosyncrasies of how Sphinx detects updates and our use of symbolic links, we recommend deleting the
> `_build` folder.

Or, you can run the following subshell command to re-generate everything without having to leave the project root:

```shell
(cd docs && rm -r _build && make html)
```

After the docs are re-generated, you can view them by starting a local file server, for example:

```shell
python -m http.server -d docs/_build/html
```
