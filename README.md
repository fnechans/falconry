# falconry

![Python package](https://github.com/fnechans/falconry/workflows/Python%20package/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/falconry/badge/?version=latest)](https://falconry.readthedocs.io/en/latest/?badge=latest)

## Introduction

Falconry is lightweight python package to create and manage your [HTCondor](https://github.com/htcondor/) jobs.
It handles things like job submission, dependent jobs, and job status checking. It periodically saves progress,
so even if you disconnect or htcondor crashes, you can continue where you left off.

Detailed documentation can be found on [ReadTheDocs](https://falconry.readthedocs.io/en/latest/index.html). You can also check `example.py` for an example of usage. Package has to be first installed using pip as described in section on [installation](#installation-using-pip).

## Instalation using pip

Falconry can be installed using pip:

    $ pip3 install falconry

## Installation from source

To install falconry, simply call following in the repository directory:

    $ pip3 install --user -e .

Then you can include the package in your project simply by adding:

    import falconry

### Installing python3 API for HTCondor

The package  requires htcondor API to run. One can simply do:

    $ python3 -m pip  install --user -r requirements.txt

though it might be better to install in virtual environment.