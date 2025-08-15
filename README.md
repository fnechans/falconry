# falconry

![Python package](https://github.com/fnechans/falconry/workflows/Python%20package/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/falconry/badge/?version=latest)](https://falconry.readthedocs.io/en/latest/?badge=latest)

## Introduction

Falconry is lightweight python package to create and manage your [HTCondor](https://github.com/htcondor/) jobs.
It handles things like job submission, dependent jobs, and job status checking. It periodically saves progress,
so even if you disconnect or htcondor crashes, you can continue where you left off.

Detailed documentation can be found on [ReadTheDocs](https://falconry.readthedocs.io/en/latest/index.html). You can also check `example.py` for an example of usage. Package has to be first installed using pip as described in section on [installation](#installation-using-pip).

For simple submition, you can also use `falconry` as an executable.

## Instalation using pip

Falconry can be installed using pip:

    $ pip3 install falconry

## Running falconry as an executable

You can do something as simple as:

    $ falconry "MY COMMAND"

This will create a job that runs `MY COMMAND` and will be submitted to HTCondor.
You might want to separate different submission by subdirectory, for that use
the -s option:

    $ falconry "MY COMMAND" -s SUBDIRECTORY

For more options, see the documentation or `falconry --help`.