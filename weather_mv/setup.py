from setuptools import setup, find_packages

base_requirements = [
    "apache-beam[gcp]",
    "numpy",
    "pandas",
    "xarray",
    "google-cloud-bigquery",
    "pyparsing==2.4.2",  # Fix http2lib auth breakage
]

setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A tool to load weather data into BigQuery.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    install_requires=base_requirements,
)
