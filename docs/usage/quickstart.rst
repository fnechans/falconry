==========
Quickstart
==========

===
Job
===

First unit of the falconry is job, imported simpl as::

    from falconry import job

Jobs require an HTCondor schedd. There is more convenient way to acquire it in the 'Manager' class mentioned later on, for now let's start with proper HTCondor python API setup::

    import htcondor
    schedd = htcondor.Schedd()

which should automatically pick-up the local schedd. The job definition then needs a name - useful for identification with a larger number of jobs - and the schedd::

    j = job(name, schedd)

There are several ways to initialize the job properties, but for a simple job, one can use a predefined function 'simple_job'::

    j.set_simple(executablaPath, logFilesPath)
)

One can setup the expected run time with 'set_time(runtime)' defined in seconds::

    j.set_time(3600)

Generally, one can add or overwrite any options to the job using 'set_custom(options)' function where options are simply dictionary::

    j.set_custom({"arguments": " --out X"})

And then to submit the job simply::

    j.submit()

=======
Manager
=======

When launching large number of jobs, especially with some dependencies between them, it is convenient to use manager class::

    from falconry import manager
    mgr = manager(dir)

The manager can save all jobs it managesin a data.json file which will be located in the specified 'dir', so one can easily start again without rerunning already finished jobs. To add a job to the manager simply do::

    mgr.add_job(j)

If you want job to start after certain other jobs finish (dependency), add them first to the job::

    j.add_dependency(j1, j2, j3)

The manager will then start the job once all dependencies are succesfully finished. Now, start the manager with following command::

    mgr.start(checkTime)

where the 'checkTime' specifies time in seconds in between checks of job status. After each interval, it will print status of each jobs and submit those waiting in queue if dependencies are satisfied.

However, user may want to interupt the programm, or there may be a crash. In that case it may be usefull to use 'start_safe()' function. It calls the 'save()' function in case of interrupt or crash, which saves all managed jobs in a data.json file. To load previous instance of the manager then simply call::

    mgr.load()

==============
Simple program
==============

An example of a complete implemenation can be found in 'example.py', which puts all these features together. It also uses command line parses to make the usage more convenient, so it automatically loads previous instance if '-- cont' command line argument is used.