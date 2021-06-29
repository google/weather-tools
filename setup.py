from setuptools import setup, find_packages


def requirements(path=''):
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        return []


setup(
    name='ecmwf-pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A GCP pipeline to make ECMWF data available to all of Alphabet.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    platforms=['darwin', 'linux'],
    python_requires='>=3.7, <4',
    install_requires=requirements('requirements/requirements.txt'),
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    extras_require={
        'dev': requirements('requirements/dev_requirements.txt')
    },
    project_urls={
        'Issue Tracking': 'https://bugdashboard.corp.google.com/app/tree;dashboardId=145842',
        'Overview': 'http://go/anthromet-ingestion',
        'Sync Meeting Notes': 'http://go/anthromet-ingestion-sync',
        'Anthromet Notes': 'http://go/anthromet-notes',
    },
)
