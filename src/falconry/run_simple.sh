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

# Using eval here is necessary when using nested commands,
# in our case e.g. when using one command to get name of rundir.
# `tp finalise $(tp --print-rundir preselect ...)`
eval "$CMD"

retval=$?

echo "Analysis error code: $retval"

exit_hook
