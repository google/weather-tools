from .pipeline import run
import typing as t


def cli(extra_args: t.Optional[t.List[str]] = None):
    """CLI entry-point for pipeline."""
    if extra_args is None:
        extra_args = []
    import sys
    run(sys.argv + extra_args)
