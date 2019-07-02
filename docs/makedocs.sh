#!/usr/bin/env bash
source activate admix
make clean
rm -r source/reference
sphinx-apidoc -o source/reference ../straxen
rm source/reference/modules.rst
make html