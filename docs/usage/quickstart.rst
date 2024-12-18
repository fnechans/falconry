==========
Quickstart
==========

---
Job
---

Basic unit of the falconry is a job, which simply mantains all properties and submition.  It can be imported simply as::

    from falconry import job

Jobs require an HTCondor schedd. There is more convenient way to acquire it in the ``manager`` class mentioned later on, for now let's set it up directly::

    from falconry import ScheddWrapper
    schedd = ScheddWrapper()

which should automatically pick-up the local schedd. The job definition then needs a name - useful for identification with a larger number of jobs - and the schedd::

    j = job(name, schedd)

There are several ways to initialize the job properties, but for a simple job, one can use a predefined function ``simple_job``::

    j.set_simple(executablaPath, logFilesPath)

which only requires path to the executable and path to a dir where the log files will be saves. Both path can be relative wrt. to the directory where the python script is run.

One can setup the expected run time with ``set_time(runtime)`` defined in seconds::

    j.set_time(3600)

Generally, one can add or overwrite any options to the job using ``set_custom(options)`` function where options are simply dictionary::

    j.set_custom({"arguments": " --out X"})

And then to submit the job simply::

    j.submit()

More details on job setup can be found in the :ref:`job` module documentation.

-------
Manager
-------

When launching large number of jobs, especially with some dependencies between them, it is convenient to use manager class. It handles all the jobs, queues and automatically submits those which are ready.

The manager can be imported as::

    from falconry import manager
    mgr = manager(dir)

It automatically finds local schedd, so jobs can be then initialized as::

    from falconry import job
    j = job(name, mgr.schedd)

without need to import HTCondor. To add a job to the manager simply do::

    mgr.add_job(j)

If you want job to start after certain other jobs finish (dependency), add them first to the job::

    j.add_dependency(j1, j2, j3)

The manager will then start the job once all dependencies are succesfully finished. This means that jobs without dependencies are submitted automatically, no need to call ``job.submit()``

Now, start the manager with following command::

    mgr.start(checkTime)

where the ``checkTime`` specifies time in seconds in between checks of job status. After each interval, it will print status of each jobs and submit those waiting in queue if dependencies are satisfied.

However, user may want to interupt the programm, or there may be a crash. For that reason falconry periodically saves all managed jobs in a data.json file via ``save()`` function of the manager. To load previous instance of the manager then simply call::

    mgr.load()

More details on manager setup can be found in the :ref:`manager` module documentation.

---------------
Example program
---------------

An example of a complete implemenation can be found in `example.py <https://github.com/fnechans/falconry/blob/master/example.py>`_, which puts all these features together. It also uses command line parser to make the usage more convenient. E.g. it automatically loads previous instance if ``--cont`` command line argument is used.
