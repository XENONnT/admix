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


setup(
    name='xe-admix',
    version='1.0.14',
    description="advanced Data Management In Xenon (aDMIX)",
    long_description=readme + '\n\n' + history,
    url='https://github.com/XENON1T/admix',
    install_requires=requirements,
    scripts=['bin/admix-download'],
    packages=find_packages(),
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
)
