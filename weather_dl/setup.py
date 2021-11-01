from setuptools import setup, find_packages

base_requirements = [
    "cdsapi",
    "ecmwf-api-client",
    "apache-beam[gcp]",
    "numpy>=1.19.1",
    "pandas",
    "xarray",
    "requests>=2.24.0",
    "firebase-admin>=5.0.0",
    "google-cloud-datastore>=1.15.0,<2",  # For compatability with apache-beam[gcp]
    "google-cloud-firestore",
    "urllib3==1.25.11",
    "pyparsing==2.4.2",  # Fix http2lib auth breakage
]

setup(
    name='download_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A tool to download weather data.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    install_requires=base_requirements,
)
