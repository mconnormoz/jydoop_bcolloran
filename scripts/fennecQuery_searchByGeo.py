import json
import jydoop
import healthreportutils_v3
import random

'''

make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /tmp/partitioned_export.7" hadoop

make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /data/fhr/tmp/3" hadoop

#did not work
make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /data/fhr/raw" hadoop

'''

def setupjob(job, args):
    """
    Set up a job to run on one or more HDFS locations

    Jobs expect one or more arguments, the HDFS path(s) to the data.
    """
    
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat as MyInputFormat

    if len(args) < 1:
        raise Exception("Usage: <hdfs-location1> [ <location2> ] [ <location3> ] [ ... ]")

    job.setInputFormatClass(MyInputFormat)
    FileInputFormat.setInputPaths(job, ",".join(args));
    job.getConfiguration().set("org.mozilla.jydoop.mappertype", "TEXT")
    # set the job to run in the RESEARCH queue
    job.getConfiguration().set("mapred.job.queue.name","research")

# setupjob=healthreportutils_v3.setupjob


@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    searchCounts = payload.daily_search_counts()

    print searchCounts

    for countData in searchCounts:
        context.write(
            tuple( list(countData[0:3])+[payload.geo] ),
            countData[3])



reduce = jydoop.sumreducer





