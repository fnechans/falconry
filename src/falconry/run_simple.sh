#!/bin/bash

# helper script for condor submission. Not intended for users.

function exit_hook() {

    echo "Returned valued $retval"

    #find .
    date

    echo "Job's done"

    exit $retval
}

echo "Running on $HOSTNAME"
date

echo "Working on process: $1"

echo "Environment"
echo "==================="
echo "basedir = $basedir"
echo "==================="

shift
CMD=$*
set --
cd $basedir

echo

echo "Executing: "
echo "$CMD"

echo

# Using bash -c to execute command; necessary for nested commands (pipes, redirects, etc.)
# NOTE: Commands are executed through the shell - ensure commands are from trusted sources
bash -c "$CMD"

retval=$?

echo "Analysis error code: $retval"

exit_hook
