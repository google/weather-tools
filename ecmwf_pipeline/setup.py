from setuptools import setup, find_packages

setup(
    name='download_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A Beam pipeline to download weather data.',
    install_requires=[
        'cdsapi',
        'ecmwf-api-client',
        'requests>=2.24.0',
    ]
)
