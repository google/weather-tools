# Contributing Guidelines

## Developer Installation

```
pip install -e .[dev]
```

Additionally, it's recommended that you add a pre-push hook to your local client.
```
cp bin/pre-push .git/hooks/
```

## Testing

```
pytype
python3 setup.py test
```

## Workflow

As of writing (2020-08), I (alxr@) have not discovered a code-review system integrated into GCP's git hosting
service. Thus, the onus for pushing non-breaking changes is on each contributor. 

To ensure code quality, please copy the pre-push hook (in the `bin/` directory) to `.git/hooks/`. This 
will type check and unit tests _locally_ before any push to the remote repository. 

Otherwise, project workflow is up to you (given your best judgement). Personal recommendation: 
- Work in feature branches. 
- When you're ready to submit, squash & merge into your primary branch (`git merge --squash <feature>`).
- Push to main as you normally would.

This project uses Google Cloud Build for continuous integration (and later, deployment). On each commit to 
the main branch, we run commands from the above two sections. Visit the [build dashboard](https://pantheon.corp.google.com/cloud-build/dashboard?project=grid-intelligence-sandbox)
to check build history and status.

## Deployment
TODO(campbellsean): Figure out how to deploy to PubSub / Dataflow / GCP services.
