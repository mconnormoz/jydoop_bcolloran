import json
import jydoop
import healthreportutils_v3
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orphanCounts_Fennec.py ./outData/orphanCounts_Fennec_2013-10-24.csv /data/fhr/nopartitions/20131022/3/part*" hadoop

----to run against full HBASE
make ARGS="scripts/orphanCounts_Fennec.py ./outData/orphanCounts_Fennec_2013-10-24.csv" hadoop

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

    if payload.version!=3:
        return

    try:
        firstDay = payload.days[0]
    except IndexError:
        return



    firstDayData = str(payload._o.get('data', {}).get('days', {}).get(firstDay,{}))
    if firstDayData=={}:
        return
    else:
        context.write(hash(firstDayData),1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer

#def reduce(key,vList,context):





