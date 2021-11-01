import logging
import sys

from .netcdf_loader import run


def cli(extra=[]):
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv + extra)
