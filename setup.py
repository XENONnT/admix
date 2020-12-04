#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

# Get requirements from requirements.txt, stripping the version tags
with open('requirements.txt') as f:
    requirements = [r.split('/')[-1] if r.startswith('git+') else r for r in f.read().splitlines()]

setup_requirements = [
    # TODO(XeBoris): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='xe-admix',
    version='0.2.0',
    description="advanced Data Management In Xenon (aDMIX)",
    long_description=readme + '\n\n' + history,
    url='https://github.com/XENON1T/admix',
    packages=find_packages(include=['admix',
                                    'admix.interfaces',
                                    'admix.tasks',
                                    'admix.helper',
                                    'admix.utils'
                                    ]),
    package_data={'admix.helper': ['defunc_'],
                  'admix': ['config/*.*', 'config/rucio_cli/*.*']},
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'admix-version=admix.admix:version',
            'admix=admix.admix:your_admix',
            'admix-download=admix.download:main',
            'admix-showrun=admix.showrun:main',
            'admix-showcontexts=admix.showcontexts:main'
        ]
    },
    license="BSD license",
    zip_safe=False,
    keywords='admix',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6'
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
