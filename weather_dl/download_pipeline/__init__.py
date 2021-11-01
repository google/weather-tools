from .pipeline import run


def cli(extra=[]):
    import sys
    run(sys.argv + extra)
