.. falconry documentation master file, created by
   sphinx-quickstart on Wed Sep 23 13:48:31 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to falconry's documentation!
====================================

HTCondor is powerful tool for managment of jobs on computation clusters. It, and especially its python API, can be a bit complicated to use for an usual user.

The goal of falconry is to have a lightweight wrapper around the HTCondor python API to run jobs. In addition, it offers a manager, which automatically submits and controls jobs, and is able to handle dependent jobs. This way one can submit large number of inter-connected jobs without having to manually run or check anything.

Falconry is running on python 3 and can be found on `GitHub <https://github.com/fnechans/falconry>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/install
   usage/quickstart
   usage/job
   usage/manager
