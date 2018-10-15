#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    # TODO: put package requirements here
]

setup_requirements = [
    # TODO(XeBoris): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='admix',
    version='0.1.0',
    description="advanced Data Managment In Xenon",
    long_description=readme + '\n\n' + history,
    author="Boris Bauermeister",
    author_email='Boris.Bauermeister@gmail.com',
    url='https://github.com/XENON1T/admix',
    packages=find_packages(include=['admix', 
                                    'admix.interfaces',
                                    ]),

    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'admix-version=admix.admix:version',
            'admix-tester=admix.admix:tester',
            #'admix-server=admix.admix:server',
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
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
        
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
