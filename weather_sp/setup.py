from setuptools import setup, find_packages

base_requirements = [
    "apache-beam[gcp]",
    "pygrib",
    "netcdf4",
]

setup(
    name='splitter_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A tool to split weather data files into per-variable files.',
    install_requires=base_requirements,
)
