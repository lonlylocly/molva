#!/bin/bash -ex

RUNDIR=/home/lonlylocly/run
SCRIPTDIR=/home/lonlylocly/woape

echo $(date)
flock -n $RUNDIR/woape.lock -c "cd $RUNDIR; python -u $SCRIPTDIR/woape.py >>$RUNDIR/woape.log 2>&1" 
