from setuptools import setup, find_packages


def requirements(path=''):
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        return []


setup(
    name='ecmwf-pipeline',
    version='0.0',
    packages=find_packages(),
    author='Anthropocene Weather Group',
    # TODO(alxr): Add group email
    # author_email='',
    url='https://source.cloud.google.com/grid-intelligence-sandbox/ecmwf-pipeline',
    description='A GCP pipeline to make ECMWF data available to all teams at Alphabet.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    platforms=['darwin', 'linux'],
    python_requires='>=3.5, <4',
    install_requires=requirements('requirements.txt'),
    extras_require={
        'dev': requirements('dev_requirements.txt')
    },
    entry_points={
        'console_scripts': [
            'ecmwf_download=ecmwf_pipeline:cli',
        ]
    },
    project_urls={
        # TODO(alxr): Find suitable bug / issue tracker
        # 'Issue Tracking': '',
    },
)
