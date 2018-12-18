#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The setup script."""

from setuptools import setup, find_packages

from glob import glob

from os.path import (basename, splitext)

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'earthengine-api',
    'daiquiri',
    'requests-oauthlib',
    'google-auth-oauthlib',
    'click',
    'dask[complete]',
    'click-configfile',
    'pendulum',
    'gee-pheno',
    'marmee',
    'oauth2client',
    'toolz'
]

dependency_links=[]

setup_requirements = [
    'pytest-runner', 'earthengine-api', 'daiquiri', 'requests-oauthlib',
    'google-auth-oauthlib', 'click', 'dask[complete]', 'click-configfile',
    'pendulum', 'gee-pheno', 'marmee', 'oauth2client', 'toolz'
]

test_requirements = [
    'pytest', 'sphinx', 'ipython', 'ipdb', 'flake8', 'doc8', 'twine'
]

setup(
    name="wapor",
    author="Francesco Bartoli",
    author_email='francesco.bartoli@geobeyond.it',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Wapor algorithms",
    version='0.2.3',
    packages=find_packages(),
    py_modules=[splitext(basename(path))[0] for path in glob('wapor/*.py')],
    install_requires=requirements,
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/francbartoli/wapor',
    license="GPLv3 license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='wapor',
    entry_points="""
        [console_scripts]
        wapor=cli.cli:main
    """,
    zip_safe=False,
)