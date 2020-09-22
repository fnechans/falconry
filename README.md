



### Installing python3 API for HTCondor

To install the python API, first figure out the version of the htcondor your cluster runs on:

    '$ condor_version'

which returns condor version in form X.Y.Z . Then install htcondor using pip

    '$ python3 -m pip  install --user htcondor==X.Y.Z'