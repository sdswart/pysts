[![Build Status](https://travis-ci.com/sdswart/pysts.svg?branch=master)](https://travis-ci.com/sdswart/pysts)          [![Coverage Status](https://coveralls.io/repos/github/sdswart/pysts/badge.svg?branch=dev)](https://coveralls.io/github/sdswart/pysts?branch=dev)          [![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)


# python-example-package

This is a starter repo for creating a new python package.

## Tests

Simply run: `pytest`


## Code coverge

Generate coverage report with: `py.test --cov=python_utils tests/`

####  before_script:
####    - bumpversion minor setup.py
####  script:
####    - pip install --user --upgrade setuptools wheel twine numpy
####    - python setup.py sdist bdist_wheel
####    - python -m twine upload --repository-url https://pypi.gtl.fyi/ dist/pysts-0.2.0*
NOTE: Delete previous builds before uploading

####  Add pypi repo:
####    - pip config --user set global.gtlpypi https://pypi.gtl.fyi/simple/

####  download:
####    - pip install --upgrade -i https://pypi.gtl.fyi/simple pysts
