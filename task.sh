#!/bin/bash -ex

#for d in "20140507" "20140508" ; do
#    ./build-profiles.py -s $d -e $d -c 1>> profiles.log 2>&1 &
#    wait $!
#    ./build-clusters.py -s $d -e $d 1>> clusters.log 2>&1 &
#    wait $!
#done
        #date "+%Y%m%d" -d "now - $d day"

SCRIPTDIR=/home/lonlylocly/woape
RUNDIR=/home/lonlylocly/run
(
    echo $(date)
    flock -n 9 || exit 1
 
    cd $RUNDIR

    for d in "24" "25" "26"; do
        #date=$(date "+%Y%m%d" -d "now - $d day")

        date="201405$d"

        $SCRIPTDIR/pre-tomita.py -s $date -e $date 1>> pre-tomita.log 2>&1  
        $SCRIPTDIR/run-tomita.py 1>> run-tomita.log 2>run-tomita.err  
        $SCRIPTDIR/parsefacts.py 1>> parsefacts.log 2>&1  
        $SCRIPTDIR/post-tomita.py -s $date -e $date 1>> post-tomita.log 2>&1 
        $SCRIPTDIR/build-profiles.py -c -s $date -e $date 1>> profiles.log 2>&1 
        $SCRIPTDIR/exclusion.py  -s $date 1>> exclusion.log 2>&1 

        for k in 100 500 100 ; do
            $SCRIPTDIR/build-clusters.py  -s $date -e $date -k $k -i 5 1>> clusters.log 2>&1 
        done

        $SCRIPTDIR/show-db-stats.py -s $date -e $date
    done

    echo "done"
) 9>./task.lock


