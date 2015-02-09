#!/usr/bin/python
import subprocess 
import logging, logging.config
from datetime import date, timedelta
from time import time
import os

logging.config.fileConfig("logging.conf")

SCRIPTDIR="/home/lonlylocly/woape"
RUNDIR="/home/lonlylocly/run"
SIMMER_JAR="Simmer-1.0-SNAPSHOT-jar-with-dependencies.jar"

def main():
    logging.info("start")
    os.chdir(RUNDIR)
    today=date.today().strftime('%Y%m%d')
    ystd=(date.today() - timedelta(1)).strftime('%Y%m%d')
    for d in (today, ystd):
        logging.info("Iteration for date: " + d)
        run("%s/pre-tomita.py -s %s -e %s 1>> pre-tomita.log 2>&1 " % (SCRIPTDIR, d, d))
        run("%s/run-tomita.py 1>> run-tomita.log 2>run-tomita.err " % SCRIPTDIR)
        run("python -m cProfile %s/parsefacts.py 1>> parsefacts.log 2>&1 " % SCRIPTDIR) 
        run("%s/post-tomita.py -s %s -e %s 1>> post-tomita.log 2>&1 " % (SCRIPTDIR, d, d))
        logging.info("done")

    run("python -m cProfile %s/current-post-cnt.py >> current-post-cnt.log 2>&1 " % SCRIPTDIR)

    run("%s/prepare-profiles.py -o profiles.json  > prepare-profiles.log 2>&1" % SCRIPTDIR) 
    run("java -Xmx700m -jar %s/%s profiles.json sims.csv > simmer.log 2>&1 " % (SCRIPTDIR, SIMMER_JAR))
    run("%s/post-profiles.py -i sims.csv  > post-profiles.log 2>&1" % SCRIPTDIR) 

    run("%s/trend.py  1>> trend.log 2>&1 " % SCRIPTDIR) 
    #run("%s/exclusion.py  1>> exclusion.log 2>&1 " % SCRIPTDIR) 

    run("python -m cProfile %s/prepare-aligner.py >> prepare-aligner.log 2>&1" % SCRIPTDIR) 
    run("python -m cProfile %s/build-clusters.py   -i 10 1>> clusters.log 2>&1 " % SCRIPTDIR) 
    run("%s/lookup.py --dir /home/lonlylocly/streaming/ 1>> lookup.log 2>&1 " % (SCRIPTDIR)) 

    logging.info("done")

def run(cmd):
    logging.info("Run: " + cmd)
    start_time=time()
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        logging.error("Failed to exec cmd: %s" % e.cmd)
        logging.info("Cmd output:\n %s" % e.output)
        end_time=time()
        logging.info("Time spent: %.3f seconds" % (end_time - start_time))
        raise Exception(e)

    end_time=time()
    logging.info("Time spent: %.3f seconds" % (end_time - start_time))

if __name__ == '__main__':
    main()

