============
Installation
============

The package  requires htcondor API to run. However, the dependency cannot be linked directly because the condor version depends on the version of htcondor your cluster uses.

To install the python API for condor using pip: ::

    $ python3 -m pip install --user requirements.txt

To install falconry, simply call following in the repository directory: ::

    $ python3 -m pip install --user -e .

Then you can include the package in your project simply by adding: ::

    import falconry

