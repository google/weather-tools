from .pipeline import run


def cli():
    """CLI entry-point for pipeline."""
    import sys
    run(sys.argv)
