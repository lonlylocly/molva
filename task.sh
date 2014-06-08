#!/bin/bash -ex

SCRIPTDIR=/home/lonlylocly/woape
RUNDIR=/home/lonlylocly/run
SIMMER_JAR=Simmer-1.0-SNAPSHOT-jar-with-dependencies.jar

do_profiles() {
    $SCRIPTDIR/prepare-profiles.py profiles.json  > prepare-profiles.log 2>&1 
    java -Xmx500m -jar target/Simmer-1.0-SNAPSHOT-jar-with-dependencies.jar profiles.json sims.csv
    $SCRIPTDIR/post-profiles.py sims.csv  > post-profiles.log 2>&1 
}

echo $(date)

for d in 1 0 ; do
    date=$(date "+%Y%m%d" -d "now - $d day")

    $SCRIPTDIR/pre-tomita.py -s $date -e $date 1>> pre-tomita.log 2>&1  
    $SCRIPTDIR/run-tomita.py 1>> run-tomita.log 2>run-tomita.err  
    $SCRIPTDIR/parsefacts.py 1>> parsefacts.log 2>&1  
    $SCRIPTDIR/post-tomita.py -s $date -e $date 1>> post-tomita.log 2>&1 

done

date=$(date "+%Y%m%d" -d "now")

$SCRIPTDIR/current-post-cnt.py >> current-post-cnt.log 2>&1 

do_profiles

$SCRIPTDIR/trend.py   1>> trend.log 2>&1 
$SCRIPTDIR/exclusion.py  -s $date 1>> exclusion.log 2>&1 

for k in 100 500 100 ; do
    $SCRIPTDIR/build-clusters.py  -s $date -e $date -k $k -i 5 1>> clusters.log 2>&1 
done

$SCRIPTDIR/show-db-stats.py -s $date -e $date

echo "done"


