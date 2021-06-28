import logging
import sys

from loader_pipeline.netcdf_loader import run

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
