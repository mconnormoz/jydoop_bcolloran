import json
import jydoop
import healthreportutils_v3
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orphanCounts_Fennec.py ./outData/fennecQuery_numberOfAddonsPerUser.csv /data/fhr/nopartitions/20131012/3/part*" hadoop

----to run against full HBASE
make ARGS="scripts/orph_Fennec_byDate.py ./outData/orphaningDatesFennec_2013-10-24.csv" hadoop

'''



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
'''

setupjob = healthreportutils_v3.setupjob



@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    firstDayData = str(payload._o.get('data', {}).get('days', {}).get(payload.days[0],{}))
    context.write(hash(firstDayData),1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer

#def reduce(key,vList,context):





