.. _pmux:

====
pmux
====

pmux (Persistent tmux) is a tool for managing persistent tmux sessions across cluster nodes.
It allows you to start a tmux session on a local or remote node, detach from it, and later reattach
to the same session. This is particularly useful for running long-running processes on cluster nodes
where you want to maintain the session even after disconnecting.

Features
--------

- Start new tmux sessions with custom commands
- Attach to existing sessions locally or via SSH
- Automatic cleanup of hostfiles when sessions are closed
- Logging of session output to log files
- Support for force-restarting sessions

Installation
------------

pmux is installed as part of the falconry package. To use it:

.. code-block:: bash

    $ python3 -m pip install --user -e .

This will install pmux as a console script that can be invoked directly:

.. code-block:: bash

    $ pmux --help

Usage
-----

.. argparse::
    :module: pmux.__main__
    :func: main
    :prog: pmux

Configuration
-------------

Hostfile Directory
~~~~~~~~~~~~~~~~~~

By default, pmux stores hostfiles (which track where sessions are running) in:

.. code-block:: bash

    ~/.local/share/persistmux/

This can be customized by modifying the ``HOSTFILE_DIR`` variable in the source code.

Logfile Directory
~~~~~~~~~~~~~~~~

Session logs (stdout/stderr from tmux panes) are stored in:

.. code-block:: bash

    ./.persistmux/

This is relative to your current working directory when starting the session.

Examples
--------

Starting a new session with a command:

.. code-block:: bash

    $ pmux myjob -- sleep 3600

This starts a tmux session named ``myjob`` running the command ``sleep 3600``.
The session will continue running even after you disconnect.

Attaching to an existing session:

.. code-block:: bash

    $ pmux -a myjob

This will attach to the existing ``myjob`` session.

Starting a session on a specific node:

.. code-block:: bash

    $ pmux myjob -- echo "Running on $(hostname)"

Then from another machine or terminal:

.. code-block:: bash

    $ pmux -a myjob

pmux will automatically detect that the session is running on a different node
and connect via SSH.

Force restarting a session:

.. code-block:: bash

    $ pmux -f myjob -- new_command

This will kill any existing session named ``myjob`` and start a new one.

Verbose mode:

.. code-block:: bash

    $ pmux -v myjob -- my_command

Enables verbose tmux output for debugging.

Viewing session logs:

.. code-block:: bash

    $ cat ./.persistmux/myjob.log

This shows the stdout/stderr output from your tmux session.

How It Works
-----------

1. When you start a session with pmux, it:
   - Creates a tmux session with your specified command
   - Writes a hostfile recording which node the session is on
   - Sets up a shell trap in the tmux session to clean up the hostfile when the command exits
   - Pipes session output to a log file
2. When you attach to an existing session:
   - pmux reads the hostfile to find which node the session is on
   - If the session is on the local node, it attaches directly
   - If the session is on a remote node, it connects via SSH and then attaches

3. Cleanup:
   - When a tmux session is closed (manually or by the command finishing),
     pmux automatically removes the hostfile
   - If a session doesn't exist but the hostfile does, pmux removes the stale hostfile

LXPlus Considerations
--------------------

If you're using pmux on CERN's lxplus cluster, make sure you have persistent tmux
sessions enabled:

https://cern.service-now.com/service-portal?id=kb_article&n=KB0008111

Without this, tmux sessions may be killed when you disconnect.

Requirements
------------

- Python 3.9+
- tmux 3.0+ (for hook support)
- pexpect (for SSH connections)
- SSH access to remote nodes (if using remote sessions)

See Also
--------

- :doc:`executable` - The main falconry executable
- :doc:`manager` - Job manager for HTCondor
