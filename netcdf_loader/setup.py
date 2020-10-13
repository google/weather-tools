from setuptools import setup, find_packages

setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A Beam pipeline to load NetCDF data into BigQuery.',
    install_requires=[
        'cdsapi',
        'ecmwf-api-client',
        'numpy',
        'pandas',
        'xarray',
        'requests>=2.24.0',
    ]
)
