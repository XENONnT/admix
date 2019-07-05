#!/usr/bin/env bash
source activate admix
make clean
#sphinx-apidoc -o ./ ../admix
sphinx-apidoc -f -H "aDMIX Module Description" -o ./ ../admix ../admix/tasks
make html