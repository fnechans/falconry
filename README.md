# falconry

![Python package](https://github.com/fnechans/falconry/workflows/Python%20package/badge.svg)

## Table of contents

- [Introduction](#introduction)
- [Installation](#installation)

## Introduction

Falconry is lightweight python package to create and manage your [HTCondor](https://github.com/htcondor/) jobs. 

Detailed documentation not available yet, check `example.py`. Package has to be first installed using pip as described in section on [installation](#installation).

## Installation

To install falconry, simply call following in the repository directory:

    $ pip3 install --user -e .

Then you can include the package in your project simply by adding:

    import falconry

### Installing python3 API for HTCondor

The package  requires htcondor API to run. However, the dependency cannot be linked directly because the condor version depends on the version of htcondor your cluster uses.

To install the python API for condor, first figure out the version of the htcondor your cluster runs on:

    $ condor_version

which returns condor version in form X.Y.Z . Then install htcondor using pip

    $ python3 -m pip  install --user htcondor==X.Y.Z
