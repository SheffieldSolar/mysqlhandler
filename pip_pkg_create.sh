#!/bin/bash
# Create pip package
# Juilan Briggs
# 16-feb-2022
# https://packaging.python.org/en/latest/tutorials/packaging-projects/


#python3 -m pip install --upgrade pip build setuptools twine wheel
rm dist/*
python3 -m build
#python3 -m twine upload -u __token__ -p $(cat ~/.test.pypi.token) --repository testpypi dist/*
python3 -m twine upload -u __token__ -p $(cat ~/.pypi.token) --repository pypi dist/*
