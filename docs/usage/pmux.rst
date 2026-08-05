.. _pmux:

====
pmux
====

pmux (Persistent tmux) is a tool for managing persistent tmux sessions across cluster nodes.
It allows you to start a tmux session on the current node, detach from it, and later reattach
to the same session from any machine (local or remote via SSH). This is particularly useful for 
running long-running processes on cluster nodes where you want to maintain the session 
even after disconnecting.

Features
--------

- Start new tmux sessions with custom commands
- Attach to existing sessions locally or via SSH
- Automatic cleanup of hostfiles when sessions are force-restarted or when attach fails
- Logging of session output to log files
- Support for force-restarting sessions (requires successful cleanup of existing session)

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

This directory can be customized by modifying the ``HOSTFILE_DIR`` variable in the source code 
(there is currently no environment variable or config file option).

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

Starting a session and attaching from another machine:

First, start a session on your current node:

.. code-block:: bash

    $ pmux myjob -- echo "Running on $(hostname)"

Then from another machine or terminal:

.. code-block:: bash

    $ pmux -a myjob

pmux will automatically read the hostfile to detect which node the session is running on
and connect via SSH if needed.

Force restarting a session:

.. code-block:: bash

    $ pmux -f myjob -- new_command

This will kill any existing session named ``myjob`` (if accessible) and start a new one.
If the existing session cannot be killed, the command will fail with an error.

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
   - Creates a tmux session with your specified command (wrapped with ``exit`` to ensure cleanup)
   - Writes a hostfile recording which node the session is on
   - Pipes session output to a log file
   - Configures tmux options (e.g., ``remain-on-exit off`` so the session closes when the command exits)
2. When you attach to an existing session:
   - pmux reads the hostfile to find which node the session is on
   - If the session is on the local node, it attaches directly
   - If the session is on a remote node, it connects via SSH and then attaches
   - If the session no longer exists, pmux removes the stale hostfile

3. Cleanup:
   - When a session is force-restarted (``-f`` flag), pmux explicitly cleans up the existing session
   - When a session doesn't exist but the hostfile does, pmux removes the stale hostfile during attach
   - If a session setup fails partway through, pmux automatically cleans up all created artifacts
   - Use ``pmux -f <job_id>`` to force cleanup of an existing session, or manually remove the hostfile

LXPlus Considerations
--------------------

If you're using pmux on CERN's lxplus cluster, make sure you have persistent tmux
sessions enabled:

https://cern.service-now.com/service-portal?id=kb_article&n=KB0008111

Without this, tmux sessions may be killed when you disconnect.

Requirements
------------

- Python 3.9+
- tmux
- pexpect (for SSH connections)
- SSH access to remote nodes (if attaching to sessions on other nodes)

See Also
--------

- :doc:`executable` - The main falconry executable
- :doc:`manager` - Job manager for HTCondor
